from fastapi import APIRouter, HTTPException, BackgroundTasks
from ...models import quiz as quiz_models
from ...database import sqlite_handler
from ...redis_client import redis_handler
import asyncio
import json
from datetime import datetime, timedelta
import logging
import html

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

router = APIRouter()

from ...services.telegram_bot import TelegramBotServiceAsync

@router.post("/start_competition", status_code=202)
async def start_competition(request: quiz_models.StartCompetitionRequest):
    logger.info(f"API: Received start competition request for bot {request.bot_token} with identifier: {request.quiz_identifier}")

    quiz_unique_id = request.quiz_identifier
    quiz_key = redis_handler.quiz_key(request.bot_token, quiz_unique_id)

    # تأكد من إنشاء جداول الإحصائيات إذا لم تكن موجودة
    await sqlite_handler.create_tables(request.stats_db_path)

    # تحديد الفئة وجلب الأسئلة
    category_to_fetch = request.category
    questions = []
    display_category_name = "عامة" # الاسم الذي سيعرض للمستخدم

    if category_to_fetch == 'General':
        questions = await sqlite_handler.get_questions_general(request.questions_db_path, request.total_questions)
        if not questions:
            logger.error(f"API: No general questions found in {request.questions_db_path} for total_questions {request.total_questions}")
            raise HTTPException(status_code=404, detail="لم يتم العثور على أي أسئلة عامة في قاعدة البيانات.")
        display_category_name = "عامة"
    elif category_to_fetch:
        questions = await sqlite_handler.get_questions_by_category(request.questions_db_path, category_to_fetch, request.total_questions)
        if not questions:
            logger.error(f"API: No questions found in category '{category_to_fetch}' for total_questions {request.total_questions}")
            raise HTTPException(status_code=404, detail=f"لم يتم العثور على أي أسئلة في فئة '{category_to_fetch}'. يرجى اختيار فئة أخرى أو إضافة أسئلة.")
        display_category_name = category_to_fetch
    else:
        logger.warning(f"API: Start request for quiz {quiz_unique_id} missing category info. Defaulting to general questions.")
        questions = await sqlite_handler.get_questions_general(request.questions_db_path, request.total_questions)
        if not questions:
            raise HTTPException(status_code=404, detail="لم يتم العثور على أي أسئلة في قاعدة البيانات (لم يتم تحديد فئة).")
        display_category_name = "عامة"

    question_ids = [q['id'] for q in questions]

    current_quiz_status = await redis_handler.get_quiz_status_by_key(quiz_key) # استخدم get_quiz_status_by_key لتقليل الاستعلام

    if not current_quiz_status:
        logger.error(f"API: Start request for non-existent quiz {quiz_unique_id}. Check if quiz was created via inline query.")
        raise HTTPException(status_code=404, detail="المسابقة غير موجودة أو انتهت صلاحيتها. يرجى إنشاء مسابقة جديدة.")

    if current_quiz_status.get("status") != "pending":
        logger.warning(f"API: Competition start request for {quiz_unique_id} rejected. Current status: {current_quiz_status.get('status')}. Expected 'pending'.")
        raise HTTPException(status_code=400, detail=f"المسابقة ليست في حالة انتظار (pending). حالتها الحالية: {current_quiz_status.get('status')}.")

    telegram_bot = TelegramBotServiceAsync(request.bot_token)
    first_question = questions[0] # أول سؤال من القائمة المفلترة

    base_question_text_for_redis = f"<b>السؤال 1</b>:\n{first_question['question']}"
    options = [first_question['opt1'], first_question['opt2'], first_question['opt3'], first_question['opt4']]
    quiz_identifier_for_callbacks = quiz_unique_id
    keyboard = {
        "inline_keyboard": [
            [{"text": opt, "callback_data": f"answer_{quiz_identifier_for_callbacks}_{first_question['id']}_{i}"}]
            for i, opt in enumerate(options)
        ]
    }

    # حساب عدد المشاركين الموجودين بالفعل في قائمة 'players'
    players_json = current_quiz_status.get('players', '[]')
    try:
        initial_participants_count = len(json.loads(players_json))
    except json.JSONDecodeError:
        logger.warning(f"API: Failed to decode players JSON for quiz {quiz_unique_id} during start. Setting initial_participants_count to 0.")
        initial_participants_count = 0

    initial_time_display = request.question_delay

    # بناء الرسالة الأولى بما في ذلك اسم الفئة
    full_initial_message_text = (
        f"❓ {base_question_text_for_redis}\n\n"
        f"🏷️ <b>الفئة</b>: {html.escape(display_category_name)}\n"
        f"👥 <b>المشاركون</b>: {initial_participants_count}\n"
        f"⏳ <b>الوقت المتبقي</b>: {initial_time_display} ثانية"
    )

    # جلب معرفات الرسالة من Redis
    inline_message_id = current_quiz_status.get('inline_message_id')
    chat_id_from_redis = current_quiz_status.get('chat_id')
    message_id_from_redis = current_quiz_status.get('message_id')

    message_params = {
        "text": full_initial_message_text,
        "reply_markup": json.dumps(keyboard),
        "parse_mode": "HTML"
    }

    # Corrected Logic: Set timer and state in Redis *before* the network call.
    # Also, correctly source `max_players` from the existing Redis state.
    max_players_from_status = int(current_quiz_status.get("max_players", 12))

    # 1. Initialize the quiz in Redis, setting its status to active.
    await redis_handler.start_quiz(
        bot_token=request.bot_token,
        quiz_unique_id=quiz_unique_id,
        questions_db_path=request.questions_db_path,
        stats_db_path=request.stats_db_path,
        question_ids=question_ids,
        time_per_question=request.question_delay,
        creator_id=int(current_quiz_status.get('creator_id', 0)),
        initial_participant_count=initial_participants_count,
        max_players=max_players_from_status
    )

    # 2. Update the quiz fields for the first question display.
    data_to_set_in_redis = {
        "current_question_text": base_question_text_for_redis,
        "current_keyboard": json.dumps(keyboard),
        "status": "active", # The quiz is now active
        "category_display_name": display_category_name,
        "current_index": 0,
        "max_players": max_players_from_status
    }
    await redis_handler.redis_client.hset(quiz_key, mapping=data_to_set_in_redis)

    # 3. Set the timer for the first question.
    end_time = datetime.now() + timedelta(seconds=request.question_delay)
    await redis_handler.set_current_question(request.bot_token, quiz_unique_id, first_question['id'], end_time)

    logger.info(f"API: [{quiz_unique_id}] State set to 'active' in Redis. Timer started. Now attempting to edit Telegram message.")

    # 4. Now, attempt to edit the message on Telegram.
    sent_message = None
    try:
        if inline_message_id:
            message_params["inline_message_id"] = inline_message_id
            sent_message = await asyncio.wait_for(telegram_bot.edit_inline_message(message_params), timeout=10.0)
        elif chat_id_from_redis and message_id_from_redis:
            message_params["chat_id"] = chat_id_from_redis
            message_params["message_id"] = message_id_from_redis
            sent_message = await asyncio.wait_for(telegram_bot.edit_message(message_params), timeout=10.0)
        else:
            logger.error(f"API: No valid message identifier found for quiz {quiz_unique_id}. Cannot send/edit message.")
            raise HTTPException(status_code=500, detail="خطأ داخلي: لم يتم العثور على معرّف الرسالة لبدء المسابقة.")

        if not sent_message.get("ok"):
            logger.error(f"API: Failed to update message in Telegram: {sent_message.get('description')}")
            raise HTTPException(status_code=500, detail=f"فشل في تعديل الرسالة الأولى في تيليجرام: {sent_message.get('description')}")

    except asyncio.TimeoutError:
        logger.error(f"API: Timed out while sending/editing first message for quiz {quiz_unique_id}. The quiz is active in Redis and will proceed.")
        # Do not raise an exception here, as the quiz has already started. The worker will handle it.
    except Exception as e:
        logger.error(f"API: Error sending/editing first message for quiz {quiz_unique_id}: {e}", exc_info=True)
        # Same as above, do not raise, let the worker handle the state.

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

    # تغيير حالة المسابقة إلى "stopping" ليقوم الـ worker بإنهاءها
    await redis_handler.redis_client.hset(quiz_key, "status", "stopping")
    logger.info(f"API: Competition {quiz_key} set to 'stopping'. Worker will finalize cleanup and results.")
    return {"message": "Competition is being stopped. Results will be posted shortly."}


@router.post("/submit_answer")
async def submit_answer(request: quiz_models.SubmitAnswerRequest):
    logger.debug(f"API: Received answer from user {request.user_id} for question {request.question_id} in quiz {request.quiz_identifier}")

    quiz_unique_id = request.quiz_identifier
    quiz_key = redis_handler.quiz_key(request.bot_token, quiz_unique_id)

    quiz_status = await redis_handler.get_quiz_status_by_key(quiz_key)

    if not quiz_status or quiz_status.get("status") not in ["active", "pending"]: # تسمح بالإجابة حتى لو كانت pending (مفيدة لاختبارات)
        if quiz_status and quiz_status.get("status") == "stopping":
            logger.warning(f"API: User {request.user_id} tried to submit answer for quiz {quiz_unique_id} which is stopping.")
            raise HTTPException(status_code=400, detail="المسابقة في طور الإيقاف حاليًا. لا يتم قبول إجابات جديدة.")
        logger.warning(f"API: User {request.user_id} tried to submit answer for inactive quiz {quiz_unique_id}.")
        raise HTTPException(status_code=400, detail="لا توجد مسابقة نشطة أو المسابقة ليست في حالة نشطة.")

    max_players = int(quiz_status.get("max_players", 12)) # استرداد الحد الأقصى للمشاركين

    players_json = quiz_status.get('players', '[]')
    try:
        players = json.loads(players_json)
        # قم بإنشاء مجموعة (set) بمعرفات اللاعبين الموجودين لتحسين أداء البحث
        participant_ids = {p['id'] for p in players if 'id' in p}

        if request.user_id not in participant_ids:
            # **المنطق الجديد: السماح بالانضمام بعد البدء**
            current_participant_count = len(participant_ids)
            if current_participant_count < max_players:
                logger.info(f"API: User {request.user_id} joining quiz {quiz_unique_id} mid-game. Current participants: {current_participant_count}/{max_players}.")
                new_player = {
                    "id": request.user_id,
                    "username": request.username,
                    "joined_at": datetime.now().isoformat(),
                    "quiz_identifier": quiz_unique_id,
                    "bot_token": request.bot_token
                }
                players.append(new_player)
                # تحديث قائمة اللاعبين في Redis
                await redis_handler.redis_client.hset(quiz_key, "players", json.dumps(players))
                # تحديث عداد المشاركين (مهم جداً للعرض والعمليات الأخرى)
                await redis_handler.redis_client.hset(quiz_key, "participant_count", len(players))
                logger.info(f"API: User {request.user_id} successfully joined quiz {quiz_unique_id}. New count: {len(players)}.")
            else:
                logger.warning(f"API: User {request.user_id} tried to join quiz {quiz_unique_id} but max players ({max_players}) reached. Denying answer.")
                raise HTTPException(status_code=403, detail=f"المسابقة ممتلئة حاليًا ({max_players} مشارك). لا يمكن الانضمام أو الإجابة.")

    except json.JSONDecodeError:
        logger.error(f"API: Failed to decode players JSON for quiz {quiz_unique_id}: {players_json}. Denying answer due to data corruption.")
        raise HTTPException(status_code=500, detail="خطأ داخلي: فشل في معالجة قائمة المشاركين.")

    quiz_time_key = redis_handler.quiz_time_key(request.bot_token, quiz_unique_id)
    quiz_time = await redis_handler.redis_client.hgetall(quiz_time_key)

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

    # تسجيل الإجابة وتحديث نقاط المستخدم في Redis
    await redis_handler.record_answer(
        bot_token=request.bot_token,
        quiz_unique_id=quiz_unique_id,
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
    quiz_status = await redis_handler.get_quiz_status_by_key(redis_handler.quiz_key(bot_token, quiz_identifier))
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

    # عدد المشاركين يتم سحبه مباشرة من 'participant_count' الذي يتم تحديثه عند الانضمام
    participants = int(quiz_status.get("participant_count", 0))

    return {
        "status": quiz_status.get("status", "inactive"),
        "current_question": int(quiz_time.get("question_id")) if quiz_time and quiz_time.get("question_id") else None,
        "total_questions": len(json.loads(quiz_status.get("question_ids", "[]"))),
        "participants": participants,
        "time_remaining": time_remaining,
    }


@router.post("/cleanup")
async def cleanup(request: quiz_models.StopCompetitionRequest):
    logger.warning(f"API: Manual cleanup requested for bot {request.bot_token} with identifier {request.quiz_identifier}.")
    await redis_handler.end_quiz(request.bot_token, request.quiz_identifier)
    return {"message": "تم تنظيف حالة Redis. ملاحظة: يتم التعامل مع حساب النتائج الكاملة وأرشفة SQLite بواسطة عامل المعالجة (worker)."}