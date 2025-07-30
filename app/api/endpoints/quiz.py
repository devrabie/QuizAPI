from fastapi import APIRouter, HTTPException, BackgroundTasks
from ...models import quiz as quiz_models
from ...database import sqlite_handler
from ...redis_client import redis_handler
import asyncio
import json
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

from ...services.telegram_bot import TelegramBotServiceAsync

@router.post("/start_competition", status_code=202)
async def start_competition(request: quiz_models.StartCompetitionRequest):
    # Log the received channel_id to confirm what PHP is sending
    logger.info(f"API: Received start competition request for bot {request.bot_token} in channel (as received): {request.channel_id}")

    # Sanity check for channel_id format (optional but good practice)
    # A real Telegram channel_id usually starts with -100
    if not str(request.channel_id).startswith('-100'):
        logger.warning(f"API: Channel ID '{request.channel_id}' does not look like a valid Telegram channel ID (does not start with -100). This might be a misconfiguration from the bot.")
        raise HTTPException(status_code=400, detail="صيغة معرّف القناة غير صالحة. يجب أن يكون معرّف قناة تيليجرام صالح (مثلًا، يبدأ بـ -100).") # Arabic message

    await sqlite_handler.create_tables(request.stats_db_path)
    questions = await sqlite_handler.get_questions(request.questions_db_path, request.total_questions)
    if not questions:
        logger.error(f"API: No questions found in {request.questions_db_path} for total_questions {request.total_questions}")
        raise HTTPException(status_code=404, detail="لم يتم العثور على أي أسئلة في قاعدة البيانات.") # Arabic message

    question_ids = [q['id'] for q in questions]
    creator_id = 0 # Placeholder for the user who started the quiz (e.g., admin_id)

    # Check if a quiz is already active for this bot/chat
    current_quiz_status = await redis_handler.get_quiz_status(request.bot_token, request.channel_id)
    if current_quiz_status:
        logger.warning(f"API: Competition already exists for bot {request.bot_token} in channel {request.channel_id}. Status: {current_quiz_status.get('status')}")
        raise HTTPException(status_code=400, detail="توجد مسابقة نشطة بالفعل أو قيد المعالجة في هذه القناة.") # Arabic message

    # Send the first question to get the message_id
    telegram_bot = TelegramBotServiceAsync(request.bot_token)
    first_question = questions[0]

    # This is the BASE text of the question (e.g., "**السؤال 1**: ما عاصمة مصر؟")
    # This part will be saved to Redis as `current_question_text` for dynamic updates.
    base_question_text_for_redis = f"**السؤال 1**: {first_question['question']}"

    options = [first_question['opt1'], first_question['opt2'], first_question['opt3'], first_question['opt4']]
    keyboard = {
        "inline_keyboard": [
            [{"text": opt, "callback_data": f"answer_{first_question['id']}_{i}"}] for i, opt in enumerate(options)
        ]
    }

    # Construct the FULL initial message text that will be SENT to Telegram.
    # It includes the question, initial participant count (0), and initial time.
    initial_participants_count = 0
    initial_time_display = request.question_delay
    full_initial_message_text = (
        f"❓ {base_question_text_for_redis}\n\n"
        f"👥 **المشاركون**: {initial_participants_count}\n"
        f"⏳ **الوقت المتبقي**: {initial_time_display} ثانية"
    )

    message_data = {
        "chat_id": request.channel_id, # This MUST be the correct Telegram channel ID from PHP
        "text": full_initial_message_text, # Send the full formatted text from the start
        "reply_markup": json.dumps(keyboard),
        "parse_mode": "Markdown"
    }

    try:
        sent_message = await telegram_bot.send_message(message_data)
        logger.info(f"API: Telegram send_message response for first question: {sent_message}")
        if not sent_message.get("ok"):
            logger.error(f"API: Failed to send first message to Telegram: {sent_message.get('description')}")
            raise HTTPException(status_code=500, detail=f"فشل في إرسال الرسالة الأولى إلى تيليجرام: {sent_message.get('description')}") # Arabic message
    except Exception as e:
        logger.error(f"API: Error sending first message for quiz: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"خطأ أثناء إرسال الرسالة الأولى للمسابقة: {e}") # Arabic message

    message_id = sent_message["result"]["message_id"]
    logger.info(f"API: First message sent successfully. Message ID: {message_id}")

    await redis_handler.start_quiz(
        bot_token=request.bot_token,
        chat_id=request.channel_id, # This correct chat_id will be stored in Redis
        message_id=message_id, # message_id is stored here
        questions_db_path=request.questions_db_path,
        stats_db_path=request.stats_db_path,
        question_ids=question_ids,
        time_per_question=request.question_delay,
        creator_id=creator_id
    )

    # Save the original BASE question text (without dynamic info) and keyboard to Redis for the worker to use.
    # The worker will combine this base text with live stats.
    quiz_key = redis_handler.quiz_key(request.bot_token, request.channel_id)
    await redis_handler.redis_client.hset(
        quiz_key, mapping={
            "current_question_text": base_question_text_for_redis, # Store only the base question text
            "current_keyboard": json.dumps(keyboard)
        }
    )

    # Set the first question's timing information
    end_time = datetime.now() + timedelta(seconds=request.question_delay)
    await redis_handler.set_current_question(request.bot_token, request.channel_id, first_question['id'], end_time)
    await redis_handler.redis_client.hset(redis_handler.quiz_key(request.bot_token, request.channel_id), "current_index", 0)

    await redis_handler.activate_quiz(request.bot_token, request.channel_id)

    logger.info(f"API: Competition started successfully for bot {request.bot_token} in channel {request.channel_id}. Quiz state saved to Redis.")
    return {"message": "Competition started."}


@router.post("/stop_competition")
async def stop_competition(request: quiz_models.StopCompetitionRequest):
    logger.info(f"API: Attempting to stop competition for bot {request.bot_token} in channel {request.channel_id}")

    quiz_key = redis_handler.quiz_key(request.bot_token, request.channel_id)

    if not await redis_handler.redis_client.exists(quiz_key):
        logger.warning(f"API: Stop request received for competition {quiz_key} that does not exist or has already been cleaned up.")
        raise HTTPException(status_code=404, detail="لا توجد مسابقة نشطة لإيقافها في هذه القناة.") # Arabic message

    quiz_status = await redis_handler.get_quiz_status_by_key(quiz_key)

    if quiz_status.get("status") != "active":
        detail_msg = f"المسابقة ليست في حالة 'نشطة' (الحالة الحالية: {quiz_status.get('status')}). لا يمكن إيقافها مباشرة عبر الواجهة البرمجية." # Arabic message
        logger.warning(f"API: [{quiz_key}] {detail_msg}")
        raise HTTPException(status_code=400, detail=detail_msg)

    await redis_handler.redis_client.hset(quiz_key, "status", "stopping")
    logger.info(f"API: Competition {quiz_key} set to 'stopping'. Worker will finalize cleanup and results.")
    return {"message": "Competition is being stopped. Results will be posted shortly."}


@router.post("/submit_answer")
async def submit_answer(request: quiz_models.SubmitAnswerRequest):
    logger.debug(f"API: Received answer from user {request.user_id} for question {request.question_id} in channel {request.channel_id}")

    # Retrieve quiz time and status
    quiz_time_key = redis_handler.quiz_time_key(request.bot_token, request.channel_id)
    quiz_time = await redis_handler.redis_client.hgetall(quiz_time_key)

    quiz_status = await redis_handler.get_quiz_status(request.bot_token, request.channel_id)
    if not quiz_status or quiz_status.get("status") != "active":
        if quiz_status and quiz_status.get("status") == "stopping":
            raise HTTPException(status_code=400, detail="المسابقة في طور الإيقاف حاليًا. لا يتم قبول إجابات جديدة.") # Arabic message
        raise HTTPException(status_code=400, detail="لا توجد مسابقة نشطة أو المسابقة ليست في حالة نشطة.") # Arabic message

    # Check if the question ID matches the current active question
    current_question_id_in_redis = int(quiz_time.get("question_id", -1)) if quiz_time and quiz_time.get("question_id") else -1
    if current_question_id_in_redis != request.question_id:
        logger.warning(f"API: User {request.user_id} submitted answer for Q{request.question_id}, but current active is Q{current_question_id_in_redis}. Or quiz_time is missing.")
        raise HTTPException(status_code=400, detail="هذا ليس السؤال النشط الحالي أو السؤال قد انتهى وقته.") # Arabic message

    # Check if user has already answered this specific question
    if await redis_handler.has_answered(request.bot_token, request.channel_id, request.question_id, request.user_id):
        logger.warning(f"API: User {request.user_id} has already answered question {request.question_id}.")
        raise HTTPException(status_code=400, detail="لقد أجبت على هذا السؤال بالفعل.") # Arabic message

    # 1. Fetch correct answer from SQLite
    questions_db_path = quiz_status.get("questions_db_path")
    if not questions_db_path:
        logger.error(f"API: Questions DB path not found in quiz status for {request.bot_token}:{request.channel_id}")
        raise HTTPException(status_code=500, detail="خطأ في تهيئة المسابقة: مسار قاعدة بيانات الأسئلة مفقود.") # Arabic message

    question = await sqlite_handler.get_question_by_id(questions_db_path, request.question_id)
    if not question:
        logger.error(f"API: Question {request.question_id} not found in DB {questions_db_path}.")
        raise HTTPException(status_code=404, detail="لم يتم العثور على السؤال.") # Arabic message

    correct_option_index = question['correct_opt'] # Assuming 0-indexed from DB
    # Options are opt1, opt2, etc. so we add 1 to the 0-indexed correct_option_index
    correct_answer_text = question[f"opt{correct_option_index + 1}"]

    score = 0
    correct = False
    if request.answer_index == correct_option_index:
        score = 1 # Assign 1 point for correct answer
        correct = True
        logger.info(f"API: User {request.user_id} answered correctly for question {request.question_id}.")
    else:
        logger.info(f"API: User {request.user_id} answered incorrectly for question {request.question_id}.")

    time_per_question = int(quiz_status.get('time_per_question', 30))

    # 2. Record answer in Redis
    await redis_handler.record_answer(
        bot_token=request.bot_token,
        chat_id=request.channel_id,
        question_id=request.question_id,
        user_id=request.user_id,
        username=request.username,
        score=score,
        time_per_question=time_per_question
    )

    return {"message": "Answer submitted.", "correct": correct, "score": score, "correct_answer_text": correct_answer_text}


@router.get("/competition_status", response_model=quiz_models.CompetitionStatusResponse)
async def competition_status(bot_token: str, channel_id: str):
    logger.debug(f"API: Checking competition status for bot {bot_token} in channel {channel_id}")
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
            logger.warning(f"API: Invalid end_time format in Redis for quiz {bot_token}:{channel_id}")


    # Calculate number of participants
    participant_keys = [key async for key in redis_handler.redis_client.scan_iter(f"QuizAnswers:{bot_token}:{channel_id}:*")]
    participants = len(participant_keys)

    return {
        "status": quiz_status.get("status", "inactive"),
        "current_question": int(quiz_time.get("question_id")) if quiz_time and quiz_time.get("question_id") else None,
        "total_questions": len(json.loads(quiz_status.get("question_ids", "[]"))),
        "participants": participants,
        "time_remaining": time_remaining,
    }


@router.get("/leaderboard", response_model=quiz_models.LeaderboardResponse)
async def leaderboard(stats_db_path: str):
    logger.debug(f"API: Getting leaderboard for stats DB: {stats_db_path}")
    board = await sqlite_handler.get_leaderboard(stats_db_path)
    return {"leaderboard": board}


@router.post("/cleanup")
async def cleanup(request: quiz_models.StopCompetitionRequest):
    logger.warning(f"API: Manual cleanup requested for bot {request.bot_token} in channel {request.channel_id}.")
    await redis_handler.end_quiz(request.bot_token, request.channel_id)
    return {"message": "تم تنظيف حالة Redis. ملاحظة: يتم التعامل مع حساب النتائج الكاملة وأرشفة SQLite بواسطة عامل المعالجة (worker)."} # Arabic message