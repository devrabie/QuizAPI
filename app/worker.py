import asyncio
import json
from datetime import datetime, timedelta
import logging
import os

# It's better to configure the root logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# It's good practice to get a logger for the specific module
logger = logging.getLogger(__name__)

# To run this worker, you need to be in the context of the parent directory of 'app'
# so that the imports work correctly.
# For example, if your structure is /path/to/project/app, you run the worker from /path/to/project
from app.redis_client import redis_handler
from app.database import sqlite_handler
from app.services.telegram_bot import TelegramBotServiceAsync

# This is a simple in-memory cache for bot instances to avoid creating them repeatedly.
bot_instances = {}

def get_telegram_bot(token: str) -> TelegramBotServiceAsync:
    if token not in bot_instances:
        bot_instances[token] = TelegramBotServiceAsync(token)
    return bot_instances[token]

async def process_active_quiz(quiz_key: str):
    logger.info(f"Processing quiz: {quiz_key}")
    quiz_status = await redis_handler.get_quiz_status_by_key(quiz_key)

    if not quiz_status or quiz_status.get("status") != "active":
        logger.warning(f"Quiz {quiz_key} is not active or has no status. Skipping.")
        return

    bot_token = quiz_status.get("bot_token")
    chat_id = quiz_key.split(":")[2] # Extract chat_id from the key

    if not bot_token:
        logger.error(f"Bot token not found for quiz {quiz_key}. Cannot proceed.")
        return

    telegram_bot = get_telegram_bot(bot_token)

    quiz_time_key = redis_handler.quiz_time_key(bot_token, chat_id)
    quiz_time = await redis_handler.redis_client.hgetall(quiz_time_key)

    # Check if the current question's time has expired
    # If quiz_time is empty, it means no question has been set yet, or it expired long ago.
    # In this case, we proceed to the next question.
    if quiz_time and "end" in quiz_time:
        try:
            end_time = datetime.fromisoformat(quiz_time["end"])
            if datetime.now() < end_time:
                # Not yet time for the next question
                return
        except ValueError as e:
            logger.error(f"Invalid end_time format for quiz {quiz_key}: {quiz_time.get('end')}. Error: {e}")
            # Consider forcing a move to the next question or ending the quiz if format is consistently bad
            pass # Proceed to handle_next_question if time parsing fails

    # If we are here, it means it's time to move to the next question or end the quiz
    await handle_next_question(quiz_key, quiz_status, telegram_bot)


async def handle_next_question(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    current_index = int(quiz_status.get("current_index", -1))
    question_ids = json.loads(quiz_status.get("question_ids", "[]"))

    next_index = current_index + 1

    bot_token = quiz_status.get("bot_token")
    chat_id = quiz_key.split(":")[2]
    message_id = int(quiz_status.get("message_id"))
    questions_db_path = quiz_status.get("questions_db_path")
    stats_db_path = quiz_status.get("stats_db_path") # Added to pass to end_quiz

    if next_index < len(question_ids):
        next_question_id = question_ids[next_index]
        question = await sqlite_handler.get_question_by_id(questions_db_path, next_question_id)

        if not question:
            logger.error(f"Question with ID {next_question_id} not found in {questions_db_path}. Ending quiz.")
            await end_quiz(quiz_key, quiz_status, telegram_bot) # Pass stats_db_path
            return

        question_text = f"**السؤال {next_index + 1}**: {question['question']}"
        options = [question['opt1'], question['opt2'], question['opt3'], question['opt4']]

        # Shuffle options to avoid predictable order, if desired
        # random.shuffle(options)
        # For now, keeping original order based on opt1, opt2, etc.

        keyboard = {
            "inline_keyboard": [
                [{"text": opt, "callback_data": f"answer_{next_question_id}_{i}"}] for i, opt in enumerate(options)
            ]
        }

        message_data = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": question_text,
            "reply_markup": json.dumps(keyboard),
            "parse_mode": "Markdown"
        }

        try:
            await telegram_bot.edit_message(message_data)
            logger.info(f"Question {next_index + 1} sent for quiz {quiz_key}")
        except Exception as e:
            logger.error(f"Failed to edit message for quiz {quiz_key}: {e}", exc_info=True)
            # If editing message fails, the quiz cannot proceed visually.
            # It's better to end it cleanly or try to resend as new message (more complex).
            await end_quiz(quiz_key, quiz_status, telegram_bot) # Pass stats_db_path
            return

        time_per_question = int(quiz_status.get("time_per_question", 30))
        end_time = datetime.now() + timedelta(seconds=time_per_question)

        await redis_handler.set_current_question(bot_token, chat_id, next_question_id, end_time)
        await redis_handler.redis_client.hset(quiz_key, "current_index", next_index)

    else:
        logger.info(f"End of questions for quiz {quiz_key}. Waiting for 10 seconds before calculating results.")
        await asyncio.sleep(10) # Wait for 10 seconds for any last-minute answers
        logger.info(f"Finishing up quiz {quiz_key}.")
        await end_quiz(quiz_key, quiz_status, telegram_bot) # Pass stats_db_path


async def end_quiz(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    bot_token = quiz_status.get("bot_token")
    chat_id = quiz_key.split(":")[2]
    message_id = int(quiz_status.get("message_id"))
    stats_db_path = quiz_status.get("stats_db_path") # Get stats db path
    questions_db_path = quiz_status.get("questions_db_path") # Get questions db path

    total_questions = len(json.loads(quiz_status.get("question_ids", "[]")))

    logger.info(f"Calculating results for quiz {quiz_key}")

    # 1. Gather results from Redis
    final_scores = {} # {user_id: {'score': N, 'username': 'name', 'answers': {q_id: score}}}
    quiz_answer_keys = await redis_handler.redis_client.keys(f"QuizAnswers:{bot_token}:{chat_id}:*")

    for key in quiz_answer_keys:
        user_id = int(key.split(":")[-1])
        user_data = await redis_handler.redis_client.hgetall(key)

        score = int(user_data.get('score', 0))
        username = user_data.get('username', f"User_{user_id}")

        # Extract individual question scores (answers.question_id)
        user_answers = {}
        for k, v in user_data.items():
            if k.startswith('answers.'):
                try:
                    q_id = int(k.split('.')[1])
                    user_answers[q_id] = int(v)
                except ValueError:
                    logger.warning(f"Malformed answer key/value in Redis for user {user_id}, key {k}: {v}")

        final_scores[user_id] = {
            'score': score,
            'username': username,
            'answers': user_answers
        }
        logger.debug(f"Collected results for user {user_id}: {final_scores[user_id]}")


    # 2. Determine winner and generate results message
    sorted_participants = sorted(final_scores.items(), key=lambda item: item[1]['score'], reverse=True)

    winner_id = None
    winner_score = 0
    winner_username = "لا يوجد"

    if sorted_participants:
        winner_id = sorted_participants[0][0]
        winner_score = sorted_participants[0][1]['score']
        winner_username = sorted_participants[0][1]['username']

    results_text = "🏆 **المسابقة انتهت! النتائج النهائية:** 🏆\n\n"
    if winner_id:
        results_text += f"🎉 **الفائز**: {winner_username} بـ {winner_score} نقطة!\n\n"
    else:
        results_text += "لم يشارك أحد في المسابقة أو لم يحصل أحد على نقاط.\n\n"

    if len(sorted_participants) > 0:
        results_text += "🏅 **لوحة المتصدرين:**\n"
        for i, (user_id, data) in enumerate(sorted_participants[:10]): # Top 10 participants
            results_text += f"{i+1}. {data['username']}: {data['score']} نقطة\n"
    else:
        results_text += "لا توجد نتائج لعرضها.\n"


    # 3. Update SQLite permanent stats and save quiz history
    logger.info(f"Saving quiz history and updating user stats for quiz {quiz_key}")
    try:
        quiz_history_id = await sqlite_handler.save_quiz_history(
            stats_db_path, chat_id, total_questions, winner_id, winner_score
        )

        for user_id, data in final_scores.items():
            total_points = data['score']
            username = data['username']
            # Correct/wrong answers need to be calculated based on individual question scores
            # For simplicity, if a score > 0 for a question, count it as correct for this specific quiz.
            correct_answers_count = sum(1 for q_score in data['answers'].values() if q_score > 0)
            wrong_answers_count = sum(1 for q_score in data['answers'].values() if q_score <= 0) # Assuming 0 or negative for wrong
            total_answers_count = len(data['answers'])

            # Update user's overall stats
            await sqlite_handler.update_user_stats(
                stats_db_path, user_id, username, total_points, correct_answers_count, wrong_answers_count
            )

            # Save participant specific data for this quiz history
            await sqlite_handler.save_quiz_participant(
                stats_db_path, quiz_history_id, user_id, total_points, data['answers']
            )
        logger.info(f"Quiz results saved to SQLite for quiz {quiz_key}.")
    except Exception as e:
        logger.error(f"Failed to save quiz results to SQLite for quiz {quiz_key}: {e}", exc_info=True)


    # 4. Send final message to Telegram
    message_data = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": results_text,
        "reply_markup": json.dumps({}), # Remove keyboard
        "parse_mode": "Markdown"
    }

    try:
        await telegram_bot.edit_message(message_data)
        logger.info(f"Final results message sent for quiz {quiz_key}")
    except Exception as e:
        logger.error(f"Failed to send final message for quiz {quiz_key}: {e}", exc_info=True)

    # 5. Clean up Redis
    await redis_handler.end_quiz(bot_token, chat_id)
    logger.info(f"Quiz {quiz_key} has ended and been cleaned up from Redis.")


async def main_loop():
    logger.info("Starting worker main loop...")
    while True:
        try:
            # Scan for all active quiz keys
            # Use 'Quiz:*:*' to get all active quizzes regardless of bot_token or chat_id
            active_quiz_keys = await redis_handler.redis_client.keys("Quiz:*:*")
            if active_quiz_keys:
                logger.info(f"Found {len(active_quiz_keys)} active quizzes.")
                # Create a task for each quiz to process them concurrently
                tasks = [process_active_quiz(key) for key in active_quiz_keys]
                await asyncio.gather(*tasks)
            else:
                logger.info("No active quizzes found. Waiting...")

        except Exception as e:
            logger.error(f"An error occurred in the main loop: {e}", exc_info=True)

        # Wait for a second before the next cycle
        await asyncio.sleep(1)


if __name__ == "__main__":
    # This allows running the worker directly, for example, in a Docker container.
    # Ensure that the Python path is set up correctly for the imports to work.
    # Example: PYTHONPATH=. python app/worker.py

    # It's useful to have a way to gracefully shut down the worker.
    # For simplicity, we'll just run the loop directly.
    # In a production environment, you'd handle signals like SIGINT and SIGTERM.

    # To run this, you must have the project root in your PYTHONPATH.
    # For example:
    # $ export PYTHONPATH=$(pwd)
    # $ python app/worker.py

    asyncio.run(main_loop())