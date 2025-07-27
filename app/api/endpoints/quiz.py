from fastapi import APIRouter, HTTPException, BackgroundTasks
from ...models import quiz as quiz_models
from ...database import sqlite_handler
from ...redis_client import redis_handler
import asyncio
from datetime import datetime, timedelta

router = APIRouter()

async def run_quiz(bot_token: str, chat_id: str, questions_db_path: str, stats_db_path: str, question_delay: int):
    # This is a background task that "runs" the quiz
    # In a real application, this would involve sending messages to the chat
    quiz_status = await redis_handler.get_quiz_status(bot_token, chat_id)
    question_ids = json.loads(quiz_status['question_ids'])

    for i, question_id in enumerate(question_ids):
        await redis_handler.redis_client.hset(redis_handler.quiz_key(bot_token, chat_id), "current_index", i)

        end_time = datetime.now() + timedelta(seconds=question_delay)
        await redis_handler.set_current_question(bot_token, chat_id, question_id, end_time)

        # Here you would typically send the question to the chat

        await asyncio.sleep(question_delay)

    # Quiz finished, clean up
    await redis_handler.end_quiz(bot_token, chat_id)
    # Persist results to SQLite (simplified)
    # This part would need more logic to gather results from Redis

@router.post("/start_competition", status_code=202)
async def start_competition(request: quiz_models.StartCompetitionRequest, background_tasks: BackgroundTasks):
    await sqlite_handler.create_tables(request.stats_db_path)
    questions = await sqlite_handler.get_questions(request.questions_db_path, request.total_questions)
    if not questions:
        raise HTTPException(status_code=404, detail="No questions found in the database.")

    question_ids = [q['id'] for q in questions]

    # Using a placeholder for creator_id
    creator_id = 0

    await redis_handler.start_quiz(request.bot_token, request.channel_id, question_ids, request.question_delay, creator_id)

    background_tasks.add_task(run_quiz, request.bot_token, request.channel_id, request.questions_db_path, request.stats_db_path, request.question_delay)

    return {"message": "Competition started."}


@router.post("/stop_competition")
async def stop_competition(request: quiz_models.StopCompetitionRequest):
    await redis_handler.end_quiz(request.bot_token, request.channel_id)
    return {"message": "Competition stopped."}


@router.post("/submit_answer")
async def submit_answer(request: quiz_models.SubmitAnswerRequest):
    quiz_time_key = redis_handler.quiz_time_key(request.bot_token, request.channel_id)
    quiz_time = await redis_handler.redis_client.hgetall(quiz_time_key)

    if not quiz_time or int(quiz_time.get("question_id", -1)) != request.question_id:
        raise HTTPException(status_code=400, detail="Question is not active.")

    if await redis_handler.has_answered(request.bot_token, request.channel_id, request.question_id, request.user_id):
        raise HTTPException(status_code=400, detail="User has already answered.")

    # In a real scenario, you'd fetch the correct answer from the DB or Redis
    # and calculate the score. Here we'll just assume the answer is correct and score is 1.
    score = 1
    correct = True

    quiz_status = await redis_handler.get_quiz_status(request.bot_token, request.channel_id)
    time_per_question = int(quiz_status.get('time_per_question', 30))

    await redis_handler.record_answer(request.bot_token, request.channel_id, request.question_id, request.user_id, score, time_per_question)

    # This should be handled in a separate process after the quiz ends
    # await sqlite_handler.update_user_stats(stats_db_path, request.user_id, request.username, score, correct)

    return {"message": "Answer submitted."}


@router.get("/competition_status", response_model=quiz_models.CompetitionStatusResponse)
async def competition_status(bot_token: str, channel_id: str):
    quiz_status = await redis_handler.get_quiz_status(bot_token, channel_id)
    if not quiz_status:
        return {"status": "inactive", "participants": 0}

    quiz_time = await redis_handler.redis_client.hgetall(redis_handler.quiz_time_key(bot_token, channel_id))

    time_remaining = None
    if quiz_time and "end" in quiz_time:
        end_time = datetime.fromisoformat(quiz_time["end"])
        remaining = end_time - datetime.now()
        time_remaining = max(0, int(remaining.total_seconds()))

    # A more complex logic would be needed to get the number of participants
    participants = 0

    return {
        "status": quiz_status.get("status", "inactive"),
        "current_question": quiz_time.get("question_id"),
        "total_questions": len(json.loads(quiz_status.get("question_ids", "[]"))),
        "participants": participants,
        "time_remaining": time_remaining,
    }


@router.get("/leaderboard", response_model=quiz_models.LeaderboardResponse)
async def leaderboard(bot_token: str, stats_db_path: str):
    # This leaderboard is based on permanent stats, not a single quiz
    board = await sqlite_handler.get_leaderboard(stats_db_path)
    return {"leaderboard": board}


@router.post("/cleanup")
async def cleanup(request: quiz_models.StopCompetitionRequest):
    # This is largely the same as stop_competition in this simplified version
    await redis_handler.end_quiz(request.bot_token, request.channel_id)
    return {"message": "Cleanup complete."}
import json
