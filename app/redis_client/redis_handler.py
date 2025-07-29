import redis.asyncio as redis
import json
import os
from datetime import datetime, timedelta

from app.worker import logger

# This should be configured from a central place (e.g., environment variables)
# For local development, it might be 'localhost' or 'redis' if using Docker Compose
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
        "status": "initializing",
        "question_ids": json.dumps(question_ids),
        "current_index": -1,
        "time_per_question": time_per_question,
        "start_time": datetime.now().isoformat(),
        "last_question_time": datetime.now().isoformat(),
        "creator_id": creator_id,
        "bot_token": bot_token,
        "message_id": message_id,
        "questions_db_path": questions_db_path,
        "stats_db_path": stats_db_path
    }
    await redis_client.hmset(key, quiz_data)

async def activate_quiz(bot_token: str, chat_id: str):
    key = quiz_key(bot_token, chat_id)
    await redis_client.hset(key, "status", "active")

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
    # The expireat should be enough to clean up quiz_time, but the worker also manages its state.
    # Add a small buffer to ensure the worker has time to process after expiry.
    await redis_client.expireat(key, end_time + timedelta(seconds=5))

async def has_answered(bot_token: str, chat_id: str, question_id: int, user_id: int) -> bool:
    key = answered_key(bot_token, chat_id, question_id, user_id)
    return await redis_client.exists(key)

# Modified to accept username
async def record_answer(bot_token: str, chat_id: str, question_id: int, user_id: int, username: str, score: int, time_per_question: int):
    answered_key_str = answered_key(bot_token, chat_id, question_id, user_id)
    # Set expiration for the answered flag slightly longer than question time
    await redis_client.setex(answered_key_str, time_per_question + 5, "true")

    answers_key = quiz_answers_key(bot_token, chat_id, user_id)

    # Store username along with score
    await redis_client.hset(answers_key, "username", username)
    await redis_client.hincrby(answers_key, "score", score)
    await redis_client.hset(answers_key, f"answers.{question_id}", score)


# --- الإصلاح الحاسم هنا ---
async def end_quiz(bot_token: str, chat_id: str):
    # Gather all related keys using specific patterns
    # Use SCAN_ITER for production to avoid blocking for many keys
    quiz_main_key = quiz_key(bot_token, chat_id)
    quiz_time_k = quiz_time_key(bot_token, chat_id)

    # Collect keys to delete
    keys_to_delete_list = [quiz_main_key, quiz_time_k]

    # Scan for user answer keys and individual answered flags
    async for key in redis_client.scan_iter(f"QuizAnswers:{bot_token}:{chat_id}:*"):
        keys_to_delete_list.append(key)
    async for key in redis_client.scan_iter(f"Answered:{bot_token}:{chat_id}:*:*"):
        keys_to_delete_list.append(key)

    # Also delete the end_quiz lock key if it exists, for robustness
    lock_key_for_end_quiz = f"Lock:EndQuiz:{quiz_main_key}" # Must match the key used in worker.py
    if await redis_client.exists(lock_key_for_end_quiz):
        keys_to_delete_list.append(lock_key_for_end_quiz)

    if keys_to_delete_list:
        # Use pipeline for atomic deletion if there are many keys, or just delete directly
        # For simplicity, direct delete is fine unless you hit performance issues
        await redis_client.delete(*keys_to_delete_list)
        logger.info(f"Redis cleanup complete for bot {bot_token}, chat {chat_id}. Deleted {len(keys_to_delete_list)} keys.")
    else:
        logger.info(f"No specific Redis keys found for cleanup for bot {bot_token}, chat {chat_id}.")