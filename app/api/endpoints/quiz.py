from fastapi import APIRouter, HTTPException, BackgroundTasks
from ...models import quiz as quiz_models
from ...database import sqlite_handler
from ...redis_client import redis_handler
import asyncio
import json # Ensure json is imported
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__) # Get logger for this module

router = APIRouter()


from ...services.telegram_bot import TelegramBotServiceAsync

@router.post("/start_competition", status_code=202)
async def start_competition(request: quiz_models.StartCompetitionRequest):
    logger.info(f"Starting competition for bot {request.bot_token} in channel {request.channel_id}")
    await sqlite_handler.create_tables(request.stats_db_path)
    questions = await sqlite_handler.get_questions(request.questions_db_path, request.total_questions)
    if not questions:
        logger.error(f"No questions found in {request.questions_db_path} for total_questions {request.total_questions}")
        raise HTTPException(status_code=404, detail="No questions found in the database.")

    question_ids = [q['id'] for q in questions]
    creator_id = 0 # Placeholder for the user who started the quiz (e.g., admin_id)

    # Check if a quiz is already active for this bot/chat
    current_quiz_status = await redis_handler.get_quiz_status(request.bot_token, request.channel_id)
    if current_quiz_status and current_quiz_status.get("status") == "active":
        raise HTTPException(status_code=400, detail="A competition is already active in this channel.")

    # Send the first question to get the message_id
    telegram_bot = TelegramBotServiceAsync(request.bot_token)
    first_question = questions[0]
    question_text = f"**السؤال 1**: {first_question['question']}"
    options = [first_question['opt1'], first_question['opt2'], first_question['opt3'], first_question['opt4']]
    keyboard = {
        "inline_keyboard": [
            [{"text": opt, "callback_data": f"answer_{first_question['id']}_{i}"}] for i, opt in enumerate(options)
        ]
    }
    message_data = {
        "chat_id": request.channel_id,
        "text": question_text,
        "reply_markup": json.dumps(keyboard),
        "parse_mode": "Markdown"
    }

    try:
        sent_message = await telegram_bot.send_message(message_data)
        if not sent_message.get("ok"):
            logger.error(f"Failed to send message to Telegram: {sent_message.get('description')}")
            raise HTTPException(status_code=500, detail=f"Failed to send message to Telegram: {sent_message.get('description')}")
    except Exception as e:
        logger.error(f"Error sending first message for quiz: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error sending first message for quiz: {e}")

    message_id = sent_message["result"]["message_id"]

    await redis_handler.start_quiz(
        bot_token=request.bot_token,
        chat_id=request.channel_id,
        message_id=message_id,
        questions_db_path=request.questions_db_path,
        stats_db_path=request.stats_db_path,
        question_ids=question_ids,
        time_per_question=request.question_delay,
        creator_id=creator_id
    )

    # Set the first question's timing information
    end_time = datetime.now() + timedelta(seconds=request.question_delay)
    await redis_handler.set_current_question(request.bot_token, request.channel_id, first_question['id'], end_time)
    await redis_handler.redis_client.hset(redis_handler.quiz_key(request.bot_token, request.channel_id), "current_index", 0)

    logger.info(f"Competition started successfully for bot {request.bot_token} in channel {request.channel_id}")
    return {"message": "Competition started."}


@router.post("/stop_competition")
async def stop_competition(request: quiz_models.StopCompetitionRequest):
    logger.info(f"Stopping competition for bot {request.bot_token} in channel {request.channel_id}")
    quiz_status = await redis_handler.get_quiz_status(request.bot_token, request.channel_id)
    if not quiz_status or quiz_status.get("status") != "active":
        raise HTTPException(status_code=400, detail="No active competition to stop in this channel.")

    # Delegate end_quiz to the worker immediately for graceful cleanup and result calculation
    # We set status to "stopping" so worker picks it up and finishes it.
    await redis_handler.redis_client.hset(redis_handler.quiz_key(request.bot_token, request.channel_id), "status", "stopping")
    logger.info(f"Competition set to 'stopping' for bot {request.bot_token} in channel {request.channel_id}. Worker will finalize.")
    return {"message": "Competition is being stopped. Results will be posted shortly."}


@router.post("/submit_answer")
async def submit_answer(request: quiz_models.SubmitAnswerRequest):
    logger.debug(f"Received answer from user {request.user_id} for question {request.question_id} in channel {request.channel_id}")

    # Retrieve quiz time and status
    quiz_time_key = redis_handler.quiz_time_key(request.bot_token, request.channel_id)
    quiz_time = await redis_handler.redis_client.hgetall(quiz_time_key)

    quiz_status = await redis_handler.get_quiz_status(request.bot_token, request.channel_id)
    if not quiz_status or quiz_status.get("status") != "active":
        raise HTTPException(status_code=400, detail="No active competition or competition is not in active state.")

    # Check if the question ID matches the current active question
    current_question_id_in_redis = int(quiz_time.get("question_id", -1)) if quiz_time else -1
    if current_question_id_in_redis != request.question_id:
        raise HTTPException(status_code=400, detail="This is not the current active question or question has expired.")

    # Check if user has already answered this specific question
    if await redis_handler.has_answered(request.bot_token, request.channel_id, request.question_id, request.user_id):
        raise HTTPException(status_code=400, detail="User has already answered this question.")

    # 1. Fetch correct answer from SQLite
    questions_db_path = quiz_status.get("questions_db_path")
    if not questions_db_path:
        logger.error(f"Questions DB path not found in quiz status for {request.bot_token}:{request.channel_id}")
        raise HTTPException(status_code=500, detail="Quiz configuration error: Questions database path missing.")

    question = await sqlite_handler.get_question_by_id(questions_db_path, request.question_id)
    if not question:
        logger.error(f"Question {request.question_id} not found in DB {questions_db_path}.")
        raise HTTPException(status_code=404, detail="Question not found.")

    correct_option_index = question['correct_opt'] # Assuming 0-indexed from DB

    score = 0
    correct = False
    if request.answer_index == correct_option_index:
        score = 1 # Assign 1 point for correct answer
        correct = True
        logger.info(f"User {request.user_id} answered correctly for question {request.question_id}.")
    else:
        logger.info(f"User {request.user_id} answered incorrectly for question {request.question_id}.")

    time_per_question = int(quiz_status.get('time_per_question', 30))

    # 2. Record answer in Redis
    await redis_handler.record_answer(
        bot_token=request.bot_token,
        chat_id=request.channel_id,
        question_id=request.question_id,
        user_id=request.user_id,
        username=request.username, # Pass username
        score=score,
        time_per_question=time_per_question
    )

    return {"message": "Answer submitted.", "correct": correct, "score": score}


@router.get("/competition_status", response_model=quiz_models.CompetitionStatusResponse)
async def competition_status(bot_token: str, channel_id: str):
    quiz_status = await redis_handler.get_quiz_status(bot_token, channel_id)
    if not quiz_status:
        return {"status": "inactive", "participants": 0, "current_question": None, "total_questions": None, "time_remaining": None}

    quiz_time = await redis_handler.redis_client.hgetall(redis_handler.quiz_time_key(bot_token, channel_id))

    time_remaining = None
    if quiz_time and "end" in quiz_time:
        try:
            end_time = datetime.fromisoformat(quiz_time["end"])
            remaining = end_time - datetime.now()
            time_remaining = max(0, int(remaining.total_seconds()))
        except ValueError:
            logger.warning(f"Invalid end_time format in Redis for quiz {bot_token}:{channel_id}")


    # Calculate number of participants
    participant_keys = await redis_handler.redis_client.keys(f"QuizAnswers:{bot_token}:{channel_id}:*")
    participants = len(participant_keys)

    return {
        "status": quiz_status.get("status", "inactive"),
        "current_question": int(quiz_time.get("question_id")) if quiz_time and quiz_time.get("question_id") else None,
        "total_questions": len(json.loads(quiz_status.get("question_ids", "[]"))),
        "participants": participants,
        "time_remaining": time_remaining,
    }


@router.get("/leaderboard", response_model=quiz_models.LeaderboardResponse)
async def leaderboard(stats_db_path: str): # bot_token is not needed for this
    # This leaderboard is based on permanent stats, not a single quiz
    board = await sqlite_handler.get_leaderboard(stats_db_path)
    return {"leaderboard": board}


@router.post("/cleanup")
async def cleanup(request: quiz_models.StopCompetitionRequest):
    # This endpoint is primarily for manual cleanup if the worker fails to complete end_quiz.
    # It just forces the Redis cleanup. The worker's end_quiz is preferred for full cleanup and archiving.
    logger.warning(f"Manual cleanup requested for bot {request.bot_token} in channel {request.channel_id}. This will NOT trigger full results calculation in SQLite.")
    await redis_handler.end_quiz(request.bot_token, request.channel_id)
    return {"message": "Redis state cleaned up. Note: Full results calculation and SQLite archiving is handled by the worker's end_quiz."}