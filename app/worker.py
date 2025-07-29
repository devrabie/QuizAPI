import asyncio
import json
from datetime import datetime, timedelta
import logging
import os

# Set up detailed logging
# To see DEBUG messages, you might need to change the level to logging.DEBUG
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Make sure imports work by running from the project root
try:
    from app.redis_client import redis_handler
    from app.database import sqlite_handler
    from app.services.telegram_bot import TelegramBotServiceAsync
except ImportError:
    # This block allows running the worker directly from within the app folder for testing
    # but the primary way should be from the project root.
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
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

    if not quiz_status:
        logger.warning(f"Quiz {quiz_key} has no status in Redis. It might have been cleaned up. Skipping.")
        return

    # A critical check: if the status is "stopping", ensure it gets cleaned up properly.
    if quiz_status.get("status") == "stopping":
        logger.info(f"Quiz {quiz_key} is in 'stopping' state. Finalizing cleanup.")
        # Attempt to finalize the quiz if it's stuck in this state
        bot_token = quiz_status.get("bot_token")
        if bot_token:
            await end_quiz(quiz_key, quiz_status, get_telegram_bot(bot_token))
        else:
            logger.error(f"Cannot finalize quiz {quiz_key} because bot_token is missing. Manual cleanup might be needed.")
        return

    if quiz_status.get("status") != "active":
        logger.info(f"Quiz {quiz_key} status is not 'active' (it is '{quiz_status.get('status')}'). Skipping question progression.")
        return

    bot_token = quiz_status.get("bot_token")
    if not bot_token:
        logger.error(f"Bot token not found for quiz {quiz_key}. Cannot proceed. Cleaning up broken state.")
        # Extract chat_id from key for cleanup
        try:
            chat_id = quiz_key.split(":")[2]
            await redis_handler.end_quiz(bot_token, chat_id)
        except Exception as e:
            logger.error(f"Failed to perform emergency cleanup for {quiz_key}: {e}")
        return

    chat_id = quiz_key.split(":")[2]
    telegram_bot = get_telegram_bot(bot_token)

    quiz_time_key = redis_handler.quiz_time_key(bot_token, chat_id)
    quiz_time = await redis_handler.redis_client.hgetall(quiz_time_key)

    # Added detailed logging for debugging
    logger.debug(f"[{quiz_key}] Full Status: {quiz_status}")
    logger.debug(f"[{quiz_key}] Timing Info: {quiz_time}")

    should_process_next_question = False
    if quiz_time and "end" in quiz_time:
        try:
            end_time = datetime.fromisoformat(quiz_time["end"])
            if datetime.now() >= end_time:
                logger.info(f"[{quiz_key}] Question timer has expired. Proceeding to next question.")
                should_process_next_question = True
            else:
                # This is normal, the question is still active.
                time_left = (end_time - datetime.now()).total_seconds()
                logger.debug(f"[{quiz_key}] Question timer has not expired yet. {time_left:.2f}s remaining. Waiting.")
                return
        except ValueError as e:
            logger.error(f"[{quiz_key}] Invalid end_time format: {quiz_time.get('end')}. Error: {e}. Forcing next question.")
            should_process_next_question = True
    else:
        # If quiz_time is empty, it means the timer for the last question expired and the key was removed, or it's an initial state error.
        logger.info(f"[{quiz_key}] No active question timer (quiz_time is empty). Assuming it's time for the next question.")
        should_process_next_question = True

    if should_process_next_question:
        await handle_next_question(quiz_key, quiz_status, telegram_bot)


async def handle_next_question(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    current_index = int(quiz_status.get("current_index", -1))
    question_ids_str = quiz_status.get("question_ids", "[]")

    try:
        question_ids = json.loads(question_ids_str)
    except json.JSONDecodeError:
        logger.error(f"[{quiz_key}] Failed to decode question_ids JSON string: {question_ids_str}. Ending quiz.")
        await end_quiz(quiz_key, quiz_status, telegram_bot)
        return

    next_index = current_index + 1

    logger.info(f"[{quiz_key}] Handling next question. Current Index: {current_index}, Next Index: {next_index}, Total Questions: {len(question_ids)}")

    if next_index < len(question_ids):
        next_question_id = question_ids[next_index]
        questions_db_path = quiz_status.get("questions_db_path")

        if not questions_db_path:
            logger.error(f"[{quiz_key}] 'questions_db_path' not found in quiz status. Cannot fetch question. Ending quiz.")
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        question = await sqlite_handler.get_question_by_id(questions_db_path, next_question_id)

        if not question:
            logger.error(f"[{quiz_key}] Question with ID {next_question_id} not found in {questions_db_path}. Ending quiz.")
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        question_text = f"**السؤال {next_index + 1}**: {question['question']}"
        options = [question['opt1'], question['opt2'], question['opt3'], question['opt4']]
        keyboard = {
            "inline_keyboard": [
                [{"text": opt, "callback_data": f"answer_{next_question_id}_{i}"}] for i, opt in enumerate(options)
            ]
        }

        chat_id = quiz_key.split(":")[2]
        message_id = int(quiz_status.get("message_id"))
        message_data = {
            "chat_id": chat_id, "message_id": message_id, "text": question_text,
            "reply_markup": json.dumps(keyboard), "parse_mode": "Markdown"
        }

        try:
            await telegram_bot.edit_message(message_data)
            logger.info(f"[{quiz_key}] Question {next_index + 1} (ID: {next_question_id}) sent/edited successfully.")
        except Exception as e:
            logger.error(f"[{quiz_key}] Failed to edit message to send Q{next_index + 1}: {e}", exc_info=True)
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        time_per_question = int(quiz_status.get("time_per_question", 30))
        end_time = datetime.now() + timedelta(seconds=time_per_question)

        bot_token = quiz_status.get("bot_token")
        await redis_handler.set_current_question(bot_token, chat_id, next_question_id, end_time)
        await redis_handler.redis_client.hset(quiz_key, "current_index", next_index)
        logger.info(f"[{quiz_key}] State updated. New current_index: {next_index}. Timer set for {time_per_question}s.")

    else:
        logger.info(f"[{quiz_key}] End of questions reached (next index {next_index} is not less than total {len(question_ids)}). Finishing up.")
        await end_quiz(quiz_key, quiz_status, telegram_bot)


async def end_quiz(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    logger.info(f"[{quiz_key}] Starting end_quiz process.")

    bot_token = quiz_status.get("bot_token")
    if not bot_token:
        logger.error(f"[{quiz_key}] Cannot end quiz, bot_token is missing.")
        # Attempt a basic cleanup if possible
        keys_to_delete = await redis_handler.redis_client.keys(f"*{quiz_key.split(':', 1)[1]}*")
        if keys_to_delete:
            await redis_handler.redis_client.delete(*keys_to_delete)
        return

    chat_id = quiz_key.split(":")[2]
    message_id = int(quiz_status.get("message_id"))
    stats_db_path = quiz_status.get("stats_db_path")

    if not stats_db_path:
        logger.error(f"[{quiz_key}] 'stats_db_path' not found. Cannot save results or send final message.")
        await redis_handler.end_quiz(bot_token, chat_id) # Just clean up redis
        return

    total_questions = len(json.loads(quiz_status.get("question_ids", "[]")))

    logger.info(f"[{quiz_key}] Calculating results from Redis.")
    final_scores = {}
    quiz_answer_keys = await redis_handler.redis_client.keys(f"QuizAnswers:{bot_token}:{chat_id}:*")

    for key in quiz_answer_keys:
        try:
            user_id = int(key.split(":")[-1])
            user_data = await redis_handler.redis_client.hgetall(key)
            score = int(user_data.get('score', 0))
            username = user_data.get('username', f"User_{user_id}")

            user_answers = {int(k.split('.')[1]): int(v) for k, v in user_data.items() if k.startswith('answers.')}

            final_scores[user_id] = {'score': score, 'username': username, 'answers': user_answers}
            logger.debug(f"[{quiz_key}] Collected results for user {user_id}: score={score}, username={username}")
        except (ValueError, IndexError) as e:
            logger.warning(f"[{quiz_key}] Could not parse user data from answer key '{key}': {e}")
            continue

    sorted_participants = sorted(final_scores.items(), key=lambda item: item[1]['score'], reverse=True)
    winner_id, winner_score, winner_username = (None, 0, "لا يوجد")
    if sorted_participants:
        winner_id, winner_data = sorted_participants[0]
        winner_score, winner_username = winner_data['score'], winner_data['username']

    results_text = "🏆 **المسابقة انتهت! النتائج النهائية:** 🏆\n\n"
    if winner_id:
        results_text += f"🎉 **الفائز**: {winner_username} بـ {winner_score} نقطة!\n\n"
    else:
        results_text += "لم يشارك أحد في المسابقة أو لم يحصل أحد على نقاط.\n\n"

    if sorted_participants:
        results_text += "🏅 **لوحة المتصدرين:**\n"
        for i, (user_id, data) in enumerate(sorted_participants[:10]):
            results_text += f"{i+1}. {data['username']}: {data['score']} نقطة\n"

    try:
        logger.info(f"[{quiz_key}] Saving quiz history and updating user stats in SQLite DB: {stats_db_path}")
        quiz_history_id = await sqlite_handler.save_quiz_history(stats_db_path, chat_id, total_questions, winner_id, winner_score)

        for user_id, data in final_scores.items():
            correct_answers_count = sum(1 for q_score in data['answers'].values() if q_score > 0)
            total_answers_in_quiz = len(data['answers'])
            wrong_answers_count = total_answers_in_quiz - correct_answers_count

            await sqlite_handler.update_user_stats(stats_db_path, user_id, data['username'], data['score'], correct_answers_count, wrong_answers_count)
            await sqlite_handler.save_quiz_participant(stats_db_path, quiz_history_id, user_id, data['score'], data['answers'])

        logger.info(f"[{quiz_key}] Quiz results saved to SQLite successfully.")
    except Exception as e:
        logger.error(f"[{quiz_key}] Failed to save quiz results to SQLite: {e}", exc_info=True)

    message_data = {"chat_id": chat_id, "message_id": message_id, "text": results_text, "reply_markup": json.dumps({}), "parse_mode": "Markdown"}
    try:
        await telegram_bot.edit_message(message_data)
        logger.info(f"[{quiz_key}] Final results message sent to Telegram.")
    except Exception as e:
        logger.error(f"[{quiz_key}] Failed to send final message: {e}", exc_info=True)

    await redis_handler.end_quiz(bot_token, chat_id)
    logger.info(f"[{quiz_key}] Quiz has ended and been cleaned up from Redis.")


async def main_loop():
    logger.info("Starting worker main loop...")
    while True:
        try:
            active_quiz_keys = await redis_handler.redis_client.keys("Quiz:*:*")
            if active_quiz_keys:
                logger.info(f"Found {len(active_quiz_keys)} quiz keys to process: {active_quiz_keys}")
                tasks = [process_active_quiz(key) for key in active_quiz_keys]
                await asyncio.gather(*tasks, return_exceptions=True) # return_exceptions prevents one failing task from stopping others
            else:
                logger.debug("No active quizzes found. Waiting...")

        except Exception as e:
            logger.error(f"An error occurred in the main loop: {e}", exc_info=True)

        await asyncio.sleep(1) # Check every 1 second


if __name__ == "__main__":
    # To run this, you must have the project root in your PYTHONPATH.
    # For example:
    # $ export PYTHONPATH=$(pwd)
    # $ python app/worker.py
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Worker shutting down gracefully.")