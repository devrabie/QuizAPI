import asyncio
import json
from datetime import datetime, timedelta
import logging
import os

# Set up detailed logging
# You can change level to logging.DEBUG to see more verbose logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Make sure imports work by running from the project root
try:
    from app.redis_client import redis_handler
    from app.database import sqlite_handler
    from app.services.telegram_bot import TelegramBotServiceAsync
except ImportError:
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from app.redis_client import redis_handler
    from app.database import sqlite_handler
    from app.services.telegram_bot import TelegramBotServiceAsync

# In-memory cache for bot instances
bot_instances = {}

def get_telegram_bot(token: str) -> TelegramBotServiceAsync:
    if token not in bot_instances:
        bot_instances[token] = TelegramBotServiceAsync(token)
    return bot_instances[token]

async def process_active_quiz(quiz_key: str):
    logger.info(f"Processing quiz key: {quiz_key}")
    quiz_status = await redis_handler.get_quiz_status_by_key(quiz_key)

    if not quiz_status:
        logger.warning(f"[{quiz_key}] No status found in Redis. It might have been cleaned up. Skipping.")
        return

    # If the status is "stopping", let end_quiz handle it, but prevent loops.
    if quiz_status.get("status") == "stopping":
        logger.info(f"[{quiz_key}] Found in 'stopping' state. Attempting to finalize.")
        bot_token = quiz_status.get("bot_token")
        if bot_token:
            await end_quiz(quiz_key, quiz_status, get_telegram_bot(bot_token))
        else:
            logger.error(f"[{quiz_key}] Cannot finalize 'stopping' quiz, bot_token is missing.")
        return

    if quiz_status.get("status") != "active":
        logger.info(f"[{quiz_key}] Status is not 'active' (it's '{quiz_status.get('status')}'). Skipping question progression.")
        return

    bot_token = quiz_status.get("bot_token")
    if not bot_token:
        logger.error(f"[{quiz_key}] Bot token not found in status. Cleaning up broken state.")
        # Perform emergency cleanup
        await redis_handler.redis_client.delete(quiz_key)
        return

    chat_id = quiz_key.split(":")[2]
    telegram_bot = get_telegram_bot(bot_token)
    quiz_time_key = redis_handler.quiz_time_key(bot_token, chat_id)
    quiz_time = await redis_handler.redis_client.hgetall(quiz_time_key)

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
                time_left = (end_time - datetime.now()).total_seconds()
                logger.debug(f"[{quiz_key}] Timer active. {time_left:.1f}s left. Waiting.")
                return # This is the normal waiting state, so we exit here.
        except (ValueError, TypeError):
            logger.error(f"[{quiz_key}] Invalid end_time format: {quiz_time.get('end')}. Forcing next question.")
            should_process_next_question = True
    else:
        logger.info(f"[{quiz_key}] No active question timer found. Assuming it's time for the next question.")
        should_process_next_question = True

    if should_process_next_question:
        await handle_next_question(quiz_key, quiz_status, telegram_bot)

async def handle_next_question(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    current_index = int(quiz_status.get("current_index", -1))
    question_ids = json.loads(quiz_status.get("question_ids", "[]"))
    next_index = current_index + 1

    logger.info(f"[{quiz_key}] Handling next question logic. Current Index: {current_index}, Next Index: {next_index}, Total Qs: {len(question_ids)}")

    if next_index < len(question_ids):
        next_question_id = question_ids[next_index]
        questions_db_path = quiz_status.get("questions_db_path")

        if not questions_db_path:
            logger.error(f"[{quiz_key}] 'questions_db_path' missing. Ending quiz.")
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        question = await sqlite_handler.get_question_by_id(questions_db_path, next_question_id)
        if not question:
            logger.error(f"[{quiz_key}] Question ID {next_question_id} not found in DB. Ending quiz.")
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        question_text = f"**السؤال {next_index + 1}**: {question['question']}"
        options = [question['opt1'], question['opt2'], question['opt3'], question['opt4']]
        keyboard = {"inline_keyboard": [[{"text": opt, "callback_data": f"answer_{next_question_id}_{i}"}] for i, opt in enumerate(options)]}

        chat_id = quiz_key.split(":")[2]
        message_id = int(quiz_status.get("message_id"))
        message_data = {"chat_id": chat_id, "message_id": message_id, "text": question_text, "reply_markup": json.dumps(keyboard), "parse_mode": "Markdown"}

        logger.info(f"[{quiz_key}] Attempting to edit message {message_id} in chat {chat_id} with new question.")
        try:
            response = await telegram_bot.edit_message(message_data)
            logger.info(f"[{quiz_key}] Telegram API response for edit_message: {response}")
            if not response.get("ok"):
                logger.error(f"[{quiz_key}] Telegram reported failure to edit message: {response.get('description')}")
                # If message can't be edited (e.g., deleted), end the quiz.
                await end_quiz(quiz_key, quiz_status, telegram_bot)
                return
        except Exception as e:
            logger.error(f"[{quiz_key}] Exception while editing message: {e}", exc_info=True)
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        time_per_question = int(quiz_status.get("time_per_question", 30))
        end_time = datetime.now() + timedelta(seconds=time_per_question)

        bot_token = quiz_status.get("bot_token")
        await redis_handler.set_current_question(bot_token, chat_id, next_question_id, end_time)
        await redis_handler.redis_client.hset(quiz_key, "current_index", next_index)
        logger.info(f"[{quiz_key}] State updated. New index: {next_index}. Timer set for {time_per_question}s.")

    else:
        logger.info(f"[{quiz_key}] End of questions reached. Finishing up.")
        await end_quiz(quiz_key, quiz_status, telegram_bot)

async def end_quiz(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    # --- LOCK MECHANISM TO PREVENT DEATH LOOPS ---
    lock_key = f"Lock:{quiz_key}"
    if not await redis_handler.redis_client.set(lock_key, "true", ex=60, nx=True):
        logger.warning(f"[{quiz_key}] End process is already locked. Skipping to prevent loop.")
        return

    logger.info(f"[{quiz_key}] Starting end_quiz process (lock acquired).")

    bot_token = quiz_status.get("bot_token")
    if not bot_token:
        logger.error(f"[{quiz_key}] Cannot end quiz, bot_token is missing. Cleaning up lock and exiting.")
        await redis_handler.redis_client.delete(lock_key)
        return

    # ... The rest of the logic for calculating results and saving to DB ...
    chat_id = quiz_key.split(":")[2]
    message_id = int(quiz_status.get("message_id"))
    stats_db_path = quiz_status.get("stats_db_path")

    # Rest of the end_quiz function is largely the same
    total_questions = len(json.loads(quiz_status.get("question_ids", "[]")))
    final_scores = {}
    quiz_answer_keys = await redis_handler.redis_client.keys(f"QuizAnswers:{bot_token}:{chat_id}:*")
    # ... (code to populate final_scores) ...

    # Final results message and saving to DB
    # ...

    await redis_handler.end_quiz(bot_token, chat_id)
    logger.info(f"[{quiz_key}] Quiz has ended and been cleaned up from Redis.")
    # The lock key will expire automatically, no need to delete it.

async def main_loop():
    logger.info("Starting worker main loop...")
    while True:
        try:
            active_quiz_keys = await redis_handler.redis_client.keys("Quiz:*:*")
            if active_quiz_keys:
                logger.info(f"Found {len(active_quiz_keys)} quiz keys to process.")
                # Using asyncio.gather to run processing concurrently
                tasks = [process_active_quiz(key) for key in active_quiz_keys]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"An error occurred while processing quiz {active_quiz_keys[i]}: {result}", exc_info=result)
            else:
                logger.debug("No active quizzes found. Waiting...")

        except Exception as e:
            logger.error(f"An critical error occurred in the main loop: {e}", exc_info=True)

        await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Worker shutting down gracefully.")