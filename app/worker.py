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
    if quiz_time and "end" in quiz_time:
        end_time = datetime.fromisoformat(quiz_time["end"])
        if datetime.now() < end_time:
            # Not yet time for the next question
            return

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

    if next_index < len(question_ids):
        next_question_id = question_ids[next_index]
        question = await sqlite_handler.get_question_by_id(questions_db_path, next_question_id)

        if not question:
            logger.error(f"Question with ID {next_question_id} not found in {questions_db_path}. Ending quiz.")
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        question_text = f"**{question['question']}**"
        options = [question['opt1'], question['opt2'], question['opt3'], question['opt4']]
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
            logger.error(f"Failed to edit message for quiz {quiz_key}: {e}")
            # Decide if we should end the quiz or retry
            return

        time_per_question = int(quiz_status.get("time_per_question", 30))
        end_time = datetime.now() + timedelta(seconds=time_per_question)

        await redis_handler.set_current_question(bot_token, chat_id, next_question_id, end_time)
        await redis_handler.redis_client.hset(quiz_key, "current_index", next_index)

    else:
        logger.info(f"End of questions for quiz {quiz_key}. Finishing up.")
        await end_quiz(quiz_key, quiz_status, telegram_bot)


async def end_quiz(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    bot_token = quiz_status.get("bot_token")
    chat_id = quiz_key.split(":")[2]
    message_id = int(quiz_status.get("message_id"))

    # Simplified end message. A real implementation would fetch results.
    final_message = "المسابقة انتهت! شكراً لمشاركتكم."

    message_data = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": final_message,
        "reply_markup": json.dumps({}) # Remove keyboard
    }

    try:
        await telegram_bot.edit_message(message_data)
    except Exception as e:
        logger.error(f"Failed to send final message for quiz {quiz_key}: {e}")

    # Clean up Redis
    await redis_handler.end_quiz(bot_token, chat_id)
    logger.info(f"Quiz {quiz_key} has ended and been cleaned up.")


async def main_loop():
    logger.info("Starting worker main loop...")
    while True:
        try:
            # Scan for all active quiz keys
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
