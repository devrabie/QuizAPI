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
    logger.info(f"API: Received start competition request for bot {request.bot_token} with identifier: {request.quiz_identifier}")

    # The quiz_identifier can now be either a Telegram channel_id or an inline quiz game ID.
    # We will use it consistently as the primary identifier for the quiz state in Redis.
    quiz_unique_id = request.quiz_identifier

    await sqlite_handler.create_tables(request.stats_db_path)
    questions = await sqlite_handler.get_questions(request.questions_db_path, request.total_questions)
    if not questions:
        logger.error(f"API: No questions found in {request.questions_db_path} for total_questions {request.total_questions}")
        raise HTTPException(status_code=404, detail="لم يتم العثور على أي أسئلة في قاعدة البيانات.")

    question_ids = [q['id'] for q in questions]
    creator_id = 0 # This will be set by PHP via inline_query and stored in Redis already for inline quizzes

    # Check if a quiz is already active for this bot/identifier
    # Use the new quiz_unique_id
    current_quiz_status = await redis_handler.get_quiz_status(request.bot_token, quiz_unique_id)
    if current_quiz_status and current_quiz_status.get("status") in ["active", "starting", "initializing"]:
        logger.warning(f"API: Competition already exists for bot {request.bot_token} with identifier {quiz_unique_id}. Status: {current_quiz_status.get('status')}")
        raise HTTPException(status_code=400, detail="توجد مسابقة نشطة بالفعل أو قيد المعالجة بهذا المعرّف.")

    telegram_bot = TelegramBotServiceAsync(request.bot_token)
    first_question = questions[0]

    base_question_text_for_redis = f"**السؤال 1**: {first_question['question']}"
    options = [first_question['opt1'], first_question['opt2'], first_question['opt3'], first_question['opt4']]
    keyboard = {
        "inline_keyboard": [
            [{"text": opt, "callback_data": f"answer_{first_question['id']}_{i}"}] for i, opt in enumerate(options)
        ]
    }

    # Initial participants count will be loaded from Redis for this quiz_unique_id (inline quizzes)
    # For a new inline quiz, the participants count would have been updated by PHP's join logic.
    existing_quiz_state = await redis_handler.redis_client.hgetall(redis_handler.quiz_key(request.bot_token, quiz_unique_id))
    players_json = existing_quiz_state.get('players', '[]')
    try:
        current_participants = len(json.loads(players_json))
    except json.JSONDecodeError:
        current_participants = 0 # Fallback if JSON is malformed

    initial_time_display = request.question_delay
    full_initial_message_text = (
        f"❓ {base_question_text_for_redis}\n\n"
        f"👥 **المشاركون**: {current_participants}\n"
        f"⏳ **الوقت المتبقي**: {initial_time_display} ثانية"
    )

    # **Crucial**: Retrieve inline_message_id or chat_id/message_id from Redis
    # These were stored by the PHP CallbackqueryCommand upon the first interaction.
    # The `quiz_unique_id` (which is `quiz_game_id` from PHP) is used as the key.
    message_identifier_data = await redis_handler.redis_client.hgetall(redis_handler.quiz_key(request.bot_token, quiz_unique_id))
    inline_message_id = message_identifier_data.get('inline_message_id')
    chat_id_from_redis = message_identifier_data.get('chat_id')
    message_id_from_redis = message_identifier_data.get('message_id')


    # Prepare message data for Telegram API (either inline or regular)
    message_params = {
        "text": full_initial_message_text,
        "reply_markup": json.dumps(keyboard),
        "parse_mode": "Markdown"
    }

    sent_message = None
    try:
        if inline_message_id:
            message_params["inline_message_id"] = inline_message_id
            sent_message = await telegram_bot.edit_inline_message(message_params) # New function needed in TelegramBotServiceAsync
            logger.info(f"API: Telegram edit_inline_message response for first question: {sent_message}")
        elif chat_id_from_redis and message_id_from_redis:
            message_params["chat_id"] = chat_id_from_redis
            message_params["message_id"] = message_id_from_redis
            sent_message = await telegram_bot.edit_message(message_params) # Existing function
            logger.info(f"API: Telegram edit_message response for first question: {sent_message}")
        else:
            logger.error(f"API: No valid message identifier (inline_message_id or chat_id/message_id) found for quiz {quiz_unique_id}.")
            raise HTTPException(status_code=500, detail="خطأ داخلي: لم يتم العثور على معرّف الرسالة لبدء المسابقة.")

        if not sent_message.get("ok"):
            logger.error(f"API: Failed to update message in Telegram: {sent_message.get('description')}")
            raise HTTPException(status_code=500, detail=f"فشل في تعديل الرسالة الأولى في تيليجرام: {sent_message.get('description')}")
    except Exception as e:
        logger.error(f"API: Error sending/editing first message for quiz {quiz_unique_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"خطأ أثناء تعديل الرسالة الأولى للمسابقة: {e}")

    # No need to get message_id from response if editing.
    # Store quiz state using quiz_unique_id
    await redis_handler.start_quiz(
        bot_token=request.bot_token,
        quiz_unique_id=quiz_unique_id, # Use quiz_unique_id here
        # No message_id parameter directly here for start_quiz, as it's now internal to redis_handler for inline
        questions_db_path=request.questions_db_path,
        stats_db_path=request.stats_db_path,
        question_ids=question_ids,
        time_per_question=request.question_delay,
        creator_id=existing_quiz_state.get('creator_id', 0) # Use creator_id from existing Redis state
    )

    quiz_key = redis_handler.quiz_key(request.bot_token, quiz_unique_id)
    await redis_handler.redis_client.hset(
        quiz_key, mapping={
            "current_question_text": base_question_text_for_redis,
            "current_keyboard": json.dumps(keyboard),
            "inline_message_id": inline_message_id, # Ensure this is also saved here for consistency
            "chat_id": chat_id_from_redis, # And chat_id
            "message_id": message_id_from_redis, # And message_id
            "status": "active" # Set status to active here
        }
    )

    end_time = datetime.now() + timedelta(seconds=request.question_delay)
    await redis_handler.set_current_question(request.bot_token, quiz_unique_id, first_question['id'], end_time)
    await redis_handler.redis_client.hset(redis_handler.quiz_key(request.bot_token, quiz_unique_id), "current_index", 0)

    # No need for activate_quiz anymore, as status is set above
    logger.info(f"API: Competition started successfully for bot {request.bot_token} with identifier {quiz_unique_id}. Quiz state saved to Redis.")
    return {"message": "Competition started."}


@router.post("/stop_competition")
async def stop_competition(request: quiz_models.StopCompetitionRequest):
    logger.info(f"API: Attempting to stop competition for bot {request.bot_token} with identifier {request.quiz_identifier}")

    quiz_key = redis_handler.quiz_key(request.bot_token, request.quiz_identifier)

    if not await redis_handler.redis_client.exists(quiz_key):
        logger.warning(f"API: Stop request received for competition {quiz_key} that does not exist or has already been cleaned up.")
        raise HTTPException(status_code=404, detail="لا توجد مسابقة نشطة لإيقافها بهذا المعرّف.")

    quiz_status = await redis_handler.get_quiz_status_by_key(quiz_key)

    if quiz_status.get("status") != "active":
        detail_msg = f"المسابقة ليست في حالة 'نشطة' (الحالة الحالية: {quiz_status.get('status')}). لا يمكن إيقافها مباشرة عبر الواجهة البرمجية."
        logger.warning(f"API: [{quiz_key}] {detail_msg}")
        raise HTTPException(status_code=400, detail=detail_msg)

    await redis_handler.redis_client.hset(quiz_key, "status", "stopping")
    logger.info(f"API: Competition {quiz_key} set to 'stopping'. Worker will finalize cleanup and results.")
    return {"message": "Competition is being stopped. Results will be posted shortly."}


@router.post("/submit_answer")
async def submit_answer(request: quiz_models.SubmitAnswerRequest):
    logger.debug(f"API: Received answer from user {request.user_id} for question {request.question_id} in quiz {request.quiz_identifier}")

    quiz_unique_id = request.quiz_identifier

    # Retrieve quiz time and status
    quiz_time_key = redis_handler.quiz_time_key(request.bot_token, quiz_unique_id)
    quiz_time = await redis_handler.redis_client.hgetall(quiz_time_key)

    quiz_status = await redis_handler.get_quiz_status(request.bot_token, quiz_unique_id)
    if not quiz_status or quiz_status.get("status") != "active":
        if quiz_status and quiz_status.get("status") == "stopping":
            raise HTTPException(status_code=400, detail="المسابقة في طور الإيقاف حاليًا. لا يتم قبول إجابات جديدة.")
        raise HTTPException(status_code=400, detail="لا توجد مسابقة نشطة أو المسابقة ليست في حالة نشطة.")

    current_question_id_in_redis = int(quiz_time.get("question_id", -1)) if quiz_time and quiz_time.get("question_id") else -1
    if current_question_id_in_redis != request.question_id:
        logger.warning(f"API: User {request.user_id} submitted answer for Q{request.question_id}, but current active is Q{current_question_id_in_redis}. Or quiz_time is missing.")
        raise HTTPException(status_code=400, detail="هذا ليس السؤال النشط الحالي أو السؤال قد انتهى وقته.")

    if await redis_handler.has_answered(request.bot_token, quiz_unique_id, request.question_id, request.user_id):
        logger.warning(f"API: User {request.user_id} has already answered question {request.question_id}.")
        raise HTTPException(status_code=400, detail="لقد أجبت على هذا السؤال بالفعل.")

    questions_db_path = quiz_status.get("questions_db_path")
    if not questions_db_path:
        logger.error(f"API: Questions DB path not found in quiz status for {request.bot_token}:{quiz_unique_id}")
        raise HTTPException(status_code=500, detail="خطأ في تهيئة المسابقة: مسار قاعدة بيانات الأسئلة مفقود.")

    question = await sqlite_handler.get_question_by_id(questions_db_path, request.question_id)
    if not question:
        logger.error(f"API: Question {request.question_id} not found in DB {questions_db_path}.")
        raise HTTPException(status_code=404, detail="لم يتم العثور على السؤال.")

    correct_option_index = question['correct_opt']
    correct_answer_text = question[f"opt{correct_option_index + 1}"]

    score = 0
    correct = False
    if request.answer_index == correct_option_index:
        score = 1
        correct = True
        logger.info(f"API: User {request.user_id} answered correctly for question {request.question_id}.")
    else:
        logger.info(f"API: User {request.user_id} answered incorrectly for question {request.question_id}.")

    time_per_question = int(quiz_status.get('time_per_question', 30))

    await redis_handler.record_answer(
        bot_token=request.bot_token,
        quiz_unique_id=quiz_unique_id, # Use quiz_unique_id
        question_id=request.question_id,
        user_id=request.user_id,
        username=request.username,
        score=score,
        time_per_question=time_per_question
    )

    return {"message": "Answer submitted.", "correct": correct, "score": score, "correct_answer_text": correct_answer_text}


@router.get("/competition_status", response_model=quiz_models.CompetitionStatusResponse)
async def competition_status(bot_token: str, quiz_identifier: str):
    logger.debug(f"API: Checking competition status for bot {bot_token} with identifier {quiz_identifier}")
    quiz_status = await redis_handler.get_quiz_status(bot_token, quiz_identifier)
    if not quiz_status:
        return {"status": "inactive", "participants": 0, "current_question": None, "total_questions": None, "time_remaining": None}

    quiz_time = await redis_handler.redis_client.hgetall(redis_handler.quiz_time_key(bot_token, quiz_identifier))

    time_remaining = None
    if quiz_time and "end" in quiz_time:
        try:
            end_time = datetime.fromisoformat(quiz_time["end"])
            remaining = end_time - datetime.now()
            time_remaining = max(0, int(remaining.total_seconds()))
        except ValueError:
            logger.warning(f"API: Invalid end_time format in Redis for quiz {bot_token}:{quiz_identifier}")

    # Calculate number of participants for the specific quiz_identifier
    # For inline quizzes, participants are stored as a JSON array in the main quiz key
    participants = 0
    if quiz_status.get('players'):
        try:
            participants = len(json.loads(quiz_status['players']))
        except json.JSONDecodeError:
            logger.warning(f"API: Failed to decode players JSON for quiz {quiz_identifier}.")

    # If it's a regular chat-based quiz (legacy), you'd scan for QuizAnswers:{bot_token}:{chat_id}:*
    # For this new inline logic, players are directly on the quiz_key until active status.
    # Once active, answers are recorded using the quiz_identifier, so the scan_iter would also work.
    # For now, let's use the `players` field stored in the initial Redis entry for pending quizzes.
    # When active, `redis_handler.record_answer` uses `quiz_unique_id`, so scan_iter below will still work
    # in the context of `QuizAnswers:{bot_token}:{quiz_unique_id}:*`.
    participant_keys = [key async for key in redis_handler.redis_client.scan_iter(f"QuizAnswers:{bot_token}:{quiz_identifier}:*")]
    participants_from_answers = len(participant_keys)

    # Use the larger of the two counts for robustness
    actual_participants_count = max(participants, participants_from_answers)


    return {
        "status": quiz_status.get("status", "inactive"),
        "current_question": int(quiz_time.get("question_id")) if quiz_time and quiz_time.get("question_id") else None,
        "total_questions": len(json.loads(quiz_status.get("question_ids", "[]"))),
        "participants": actual_participants_count, # Use the actual count
        "time_remaining": time_remaining,
    }


@router.post("/cleanup")
async def cleanup(request: quiz_models.StopCompetitionRequest):
    logger.warning(f"API: Manual cleanup requested for bot {request.bot_token} with identifier {request.quiz_identifier}.")
    await redis_handler.end_quiz(request.bot_token, request.quiz_identifier)
    return {"message": "تم تنظيف حالة Redis. ملاحظة: يتم التعامل مع حساب النتائج الكاملة وأرشفة SQLite بواسطة عامل المعالجة (worker)."}