import redis.asyncio as redis
import json
import os
from datetime import datetime, timedelta
import logging # Import logging

# Get a logger for this module
logger = logging.getLogger(__name__) # Use specific logger for redis_client

# This should be configured from a central place (e.g., environment variables)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def quiz_key(bot_token: str, chat_id: str) -> str:
    return f"Quiz:{bot_token}:{chat_id}"

def quiz_answers_key(bot_token: str, chat_id: str, user_id: int) -> str:
    return f"QuizAnswers:{bot_token}:{chat_id}:{user_id}"

def quiz_results_key(bot_token: str, chat_id: str) -> str:
    return f"QuizResults:{bot_token}:{chat_id}"

def quiz_time_key(bot_token: str, chat_id: str) -> str:
    return f"QuizTime:{bot_token}:{chat_id}"

def answered_key(bot_token: str, chat_id: str, question_id: int, user_id: int) -> str:
    return f"Answered:{bot_token}:{chat_id}:{question_id}:{user_id}"

async def start_quiz(bot_token: str, chat_id: str, message_id: int, questions_db_path: str, stats_db_path: str, question_ids: list, time_per_question: int, creator_id: int):
    key = quiz_key(bot_token, chat_id)
    quiz_data = {

        "status": "initializing", # Set initial status
        "chat_id": chat_id,
        "question_ids": json.dumps(question_ids),
        "current_index": -1, # Set initial index
        "time_per_question": time_per_question,
        "start_time": datetime.now().isoformat(),
        "last_question_time": datetime.now().isoformat(),
        "creator_id": creator_id,
        "bot_token": bot_token,
        "message_id": message_id, # message_id is correctly saved here
        "questions_db_path": questions_db_path,
        "stats_db_path": stats_db_path
    }
    await redis_client.hmset(key, quiz_data)
    logger.info(f"Redis: Quiz {key} initialized with message_id {message_id}.")

async def activate_quiz(bot_token: str, chat_id: str):
    key = quiz_key(bot_token, chat_id)
    await redis_client.hset(key, "status", "active")
    logger.info(f"Redis: Quiz {key} status set to 'active'.")

async def get_quiz_status(bot_token: str, chat_id: str):
    key = quiz_key(bot_token, chat_id)
    return await redis_client.hgetall(key)

async def get_quiz_status_by_key(key: str):
    return await redis_client.hgetall(key)

async def set_current_question(bot_token: str, chat_id: str, question_id: int, end_time: datetime):
    key = quiz_time_key(bot_token, chat_id)
    time_data = {
        "question_id": question_id,
        "start": datetime.now().isoformat(),
        "end": end_time.isoformat()
    }
    await redis_client.hmset(key, time_data)
    # The expireat is important for automatic cleanup of quiz_time if worker fails.
    await redis_client.expireat(key, end_time + timedelta(seconds=5))
    logger.info(f"Redis: Current question set to {question_id} for {quiz_time_key(bot_token, chat_id)}. Expires at {end_time.isoformat()}.")


async def has_answered(bot_token: str, chat_id: str, question_id: int, user_id: int) -> bool:
    key = answered_key(bot_token, chat_id, question_id, user_id)
    return await redis_client.exists(key)

async def record_answer(bot_token: str, chat_id: str, question_id: int, user_id: int, username: str, score: int, time_per_question: int):
    answered_key_str = answered_key(bot_token, chat_id, question_id, user_id)
    await redis_client.setex(answered_key_str, time_per_question + 5, "true")

    answers_key = quiz_answers_key(bot_token, chat_id, user_id)
    await redis_client.hset(answers_key, "username", username)
    await redis_client.hincrby(answers_key, "score", score)
    await redis_client.hset(answers_key, f"answers.{question_id}", score)
    logger.debug(f"Redis: Recorded answer for user {user_id} on Q{question_id} (score: {score}) in quiz {chat_id}.")

# --- MODIFIED end_quiz function for full cleanup ---
async def end_quiz(bot_token: str, chat_id: str):
    logger.info(f"Redis: Initiating full Redis cleanup for bot {bot_token}, chat {chat_id}.")

    keys_to_delete_list = []

    # Add main quiz keys
    main_quiz_key = quiz_key(bot_token, chat_id)
    quiz_time_key_str = quiz_time_key(bot_token, chat_id)
    end_quiz_lock_key = f"Lock:EndQuiz:{main_quiz_key}" # Ensure this matches worker.py

    keys_to_delete_list.extend([main_quiz_key, quiz_time_key_str, end_quiz_lock_key])

    # Scan for user answer keys and individual answered flags related to this specific quiz
    async for key in redis_client.scan_iter(f"QuizAnswers:{bot_token}:{chat_id}:*"):
        keys_to_delete_list.append(key)
    async for key in redis_client.scan_iter(f"Answered:{bot_token}:{chat_id}:*:*"):
        keys_to_delete_list.append(key)

    # Filter out potential None values (though not expected if keys are correctly generated)
    keys_to_delete_list = [k for k in keys_to_delete_list if k is not None]

    if keys_to_delete_list:
        deleted_count = await redis_client.delete(*keys_to_delete_list)
        logger.info(f"Redis: Cleanup complete for bot {bot_token}, chat {chat_id}. Deleted {deleted_count} keys.")
    else:
        logger.info(f"Redis: No specific Redis keys found for cleanup for bot {bot_token}, chat {chat_id}.")