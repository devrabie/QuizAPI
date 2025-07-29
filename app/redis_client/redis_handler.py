import redis.asyncio as redis
import json
import os
from datetime import datetime, timedelta

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
        "status": "active",
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


async def end_quiz(bot_token: str, chat_id: str):
    # This is a simplified cleanup. In a real scenario, you might want to archive results.
    # The worker handles archiving to SQLite before calling this.
    keys_to_delete = await redis_client.keys(f"Quiz*{bot_token}:{chat_id}*")
    if keys_to_delete:
        await redis_client.delete(*keys_to_delete)