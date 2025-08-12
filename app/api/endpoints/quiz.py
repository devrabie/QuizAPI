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
    logger.info(f"API: تم استلام طلب بدء المسابقة للبوت {request.bot_token} بمعرف: {request.quiz_identifier}")

    quiz_unique_id = request.quiz_identifier
    quiz_key = redis_handler.quiz_key(request.bot_token, quiz_unique_id)

    await sqlite_handler.ensure_db_schema_latest(request.stats_db_path)

    category_to_fetch = request.category
    questions = []
    display_category_name = "عامة"

    if category_to_fetch == 'General':
        questions = await sqlite_handler.get_questions_general(request.questions_db_path, request.total_questions)
        if not questions:
            logger.error(f"API: لم يتم العثور على أسئلة عامة في {request.questions_db_path} لعدد الأسئلة {request.total_questions}")
            raise HTTPException(status_code=404, detail="لم يتم العثور على أي أسئلة عامة في قاعدة البيانات.")
        display_category_name = "عامة"
    elif category_to_fetch:
        questions = await sqlite_handler.get_questions_by_category(request.questions_db_path, category_to_fetch, request.total_questions)
        if not questions:
            logger.error(f"API: لم يتم العثور على أسئلة في الفئة '{category_to_fetch}' لعدد الأسئلة {request.total_questions}")
            raise HTTPException(status_code=404, detail=f"لم يتم العثور على أي أسئلة في فئة '{category_to_fetch}'. يرجى اختيار فئة أخرى أو إضافة أسئلة.")
        display_category_name = category_to_fetch
    else:
        logger.warning(f"API: طلب بدء المسابقة {quiz_unique_id} يفتقر إلى معلومات الفئة. الافتراضي هو أسئلة عامة.")
        questions = await sqlite_handler.get_questions_general(request.questions_db_path, request.total_questions)
        if not questions:
            raise HTTPException(status_code=404, detail="لم يتم العثور على أي أسئلة في قاعدة البيانات (لم يتم تحديد فئة).")
        display_category_name = "عامة"

    question_ids = [q['id'] for q in questions]

    current_quiz_status = await redis_handler.get_quiz_status_by_key(quiz_key)

    if not current_quiz_status:
        logger.error(f"API: طلب بدء المسابقة {quiz_unique_id} لمسابقة غير موجودة. تحقق مما إذا كانت المسابقة قد تم إنشاؤها عبر استعلام مضمن.")
        raise HTTPException(status_code=404, detail="المسابقة غير موجودة أو انتهت صلاحيتها. يرجى إنشاء مسابقة جديدة.")

    if current_quiz_status.get("status") != "pending":
        logger.warning(f"API: طلب بدء المسابقة {quiz_unique_id} مرفوض. الحالة الحالية: {current_quiz_status.get('status')}. المتوقع 'pending'.")
        raise HTTPException(status_code=400, detail=f"المسابقة ليست في حالة انتظار (pending). حالتها الحالية: {current_quiz_status.get('status')}.")

    players_json = current_quiz_status.get('players', '[]')
    try:
        initial_participants_count = len(json.loads(players_json))
    except json.JSONDecodeError:
        logger.warning(f"API: فشل فك ترميز JSON للاعبين للمسابقة {quiz_unique_id} أثناء البدء. تم تعيين عدد المشاركين الأولي إلى 0.")
        initial_participants_count = 0

    # **الشرط الجديد: عدم قبول بدء المسابقة إذا كان المشاركون لاعبًا واحدًا أو أقل**
    if initial_participants_count < 2:
        logger.warning(f"API: طلب بدء المسابقة {quiz_unique_id} مرفوض. {initial_participants_count} مشارك فقط. مطلوب اثنان على الأقل.")
        raise HTTPException(status_code=400, detail="يجب أن يكون هناك لاعبان اثنان على الأقل لبدء المسابقة.")

    telegram_bot = TelegramBotServiceAsync(request.bot_token)
    first_question = questions[0]

    base_question_text_for_redis = f"<b>السؤال 1</b>:\n{first_question['question']}"
    options = [first_question['opt1'], first_question['opt2'], first_question['opt3'], first_question['opt4']]
    quiz_identifier_for_callbacks = quiz_unique_id
    keyboard = {
        "inline_keyboard": [
            [{"text": opt, "callback_data": f"answer_{quiz_identifier_for_callbacks}_{first_question['id']}_{i}"}]
            for i, opt in enumerate(options)
        ]
    }

    inline_message_id = current_quiz_status.get('inline_message_id')
    chat_id_from_redis = current_quiz_status.get('chat_id')
    message_id_from_redis = current_quiz_status.get('message_id')

    # استخدم `request.chat_id` كمعرف المحادثة الرئيسي إذا تم توفيره، وإلا حاول استخدامه من Redis
    actual_chat_id = request.chat_id if request.chat_id is not None else (int(chat_id_from_redis) if chat_id_from_redis else None)

    initial_time_display = request.question_delay

    full_initial_message_text = (
        f"❓ {base_question_text_for_redis}\n\n"
        f"🏷️ <b>الفئة</b>: {html.escape(display_category_name)}\n"
        f"👥 <b>المشاركون</b>: {initial_participants_count}\n"
        f"⏳ <b>الوقت المتبقي</b>: {initial_time_display} ثانية"
    )

    message_params = {
        "text": full_initial_message_text,
        "reply_markup": json.dumps(keyboard),
        "parse_mode": "HTML"
    }

    max_players_from_status = int(current_quiz_status.get("max_players", 12))

    await redis_handler.start_quiz(
        bot_token=request.bot_token,
        quiz_unique_id=quiz_unique_id,
        questions_db_path=request.questions_db_path,
        stats_db_path=request.stats_db_path,
        question_ids=question_ids,
        time_per_question=request.question_delay,
        creator_id=int(current_quiz_status.get('creator_id', 0)),
        initial_participant_count=initial_participants_count,
        max_players=max_players_from_status,
        eliminate_after=request.eliminate_after_x_wrong,
        base_score=request.base_score_per_question,
        time_bonus=request.time_bonus_per_question,
        show_correct_answer_on_wrong=request.show_correct_answer_on_wrong,
        chat_id=actual_chat_id # تمرير معرف المحادثة
    )

    data_to_set_in_redis = {
        "current_question_text": base_question_text_for_redis,
        "current_keyboard": json.dumps(keyboard),
        "status": "active",
        "category_display_name": display_category_name,
        "current_index": 0,
        "mid_update_sent": "0",
        "max_players": max_players_from_status
    }
    await redis_handler.redis_client.hset(quiz_key, mapping=data_to_set_in_redis)

    end_time = datetime.now() + timedelta(seconds=request.question_delay)
    await redis_handler.set_current_question(request.bot_token, quiz_unique_id, first_question['id'], end_time)

    logger.info(f"API: [{quiz_unique_id}] تم تعيين الحالة إلى 'نشطة' في Redis. بدأ المؤقت. جاري محاولة تعديل رسالة تيليجرام.")

    try:
        if inline_message_id:
            message_params["inline_message_id"] = inline_message_id
            await asyncio.wait_for(telegram_bot.edit_inline_message(message_params), timeout=10.0)
        elif chat_id_from_redis and message_id_from_redis:
            message_params["chat_id"] = chat_id_from_redis
            message_params["message_id"] = message_id_from_redis
            await asyncio.wait_for(telegram_bot.edit_message(message_params), timeout=10.0)
        else:
            logger.error(f"API: لم يتم العثور على معرّف رسالة صالح للمسابقة {quiz_unique_id}. لا يمكن إرسال/تعديل الرسالة.")
            # لا ترفع استثناء هنا، فالمسابقة بدأت بالفعل في Redis. العامل سيتعامل معها.

    except asyncio.TimeoutError:
        logger.error(f"API: انتهت مهلة إرسال/تعديل الرسالة الأولى للمسابقة {quiz_unique_id}. المسابقة نشطة في Redis وستستمر.")
        pass
    except Exception as e:
        logger.error(f"API: خطأ في إرسال/تعديل الرسالة الأولى للمسابقة {quiz_unique_id}: {e}", exc_info=True)
        pass

    logger.info(f"API: بدأت المسابقة بنجاح للبوت {request.bot_token} بمعرف {quiz_unique_id}. تم حفظ حالة المسابقة في Redis.")
    return {"message": "بدأت المسابقة."}


@router.post("/stop_competition")
async def stop_competition(request: quiz_models.StopCompetitionRequest):
    logger.info(f"API: محاولة إيقاف المسابقة للبوت {request.bot_token} بمعرف {request.quiz_identifier}")

    quiz_key = redis_handler.quiz_key(request.bot_token, request.quiz_identifier)

    if not await redis_handler.redis_client.exists(quiz_key):
        logger.warning(f"API: تم استلام طلب إيقاف للمسابقة {quiz_key} غير الموجودة أو التي تم تنظيفها بالفعل.")
        raise HTTPException(status_code=404, detail="لا توجد مسابقة نشطة لإيقافها بهذا المعرّف.")

    quiz_status = await redis_handler.get_quiz_status_by_key(quiz_key)

    if quiz_status.get("status") != "active":
        detail_msg = f"المسابقة ليست في حالة 'نشطة' (الحالة الحالية: {quiz_status.get('status')}). لا يمكن إيقافها مباشرة عبر الواجهة البرمجية."
        logger.warning(f"API: [{quiz_key}] {detail_msg}")
        raise HTTPException(status_code=400, detail=detail_msg)

    await redis_handler.redis_client.hset(quiz_key, "status", "stopping")
    logger.info(f"API: تم تعيين المسابقة {quiz_key} إلى 'stopping'. سيقوم العامل بإنهاء التنظيف والنتائج.")
    return {"message": "المسابقة قيد الإيقاف. سيتم نشر النتائج قريباً."}


@router.post("/submit_answer")
async def submit_answer(request: quiz_models.SubmitAnswerRequest):
    logger.debug(f"API: تم استلام إجابة من المستخدم {request.user_id} للسؤال {request.question_id} في المسابقة {request.quiz_identifier}")

    quiz_unique_id = request.quiz_identifier
    quiz_key = redis_handler.quiz_key(request.bot_token, quiz_unique_id)

    quiz_status = await redis_handler.get_quiz_status_by_key(quiz_key)

    if not quiz_status or quiz_status.get("status") not in ["active", "pending"]:
        if quiz_status and quiz_status.get("status") == "stopping":
            logger.warning(f"API: حاول المستخدم {request.user_id} تقديم إجابة للمسابقة {quiz_unique_id} وهي قيد الإيقاف.")
            raise HTTPException(status_code=400, detail="المسابقة في طور الإيقاف حاليًا. لا يتم قبول إجابات جديدة.")
        logger.warning(f"API: حاول المستخدم {request.user_id} تقديم إجابة لمسابقة غير نشطة {quiz_unique_id}.")
        raise HTTPException(status_code=400, detail="لا توجد مسابقة نشطة أو المسابقة ليست في حالة نشطة.")

    max_players = int(quiz_status.get("max_players", 12))
    time_per_question = int(quiz_status.get('time_per_question', 30))
    base_score_per_question = float(quiz_status.get('base_score', 1.0))
    time_bonus_per_question = float(quiz_status.get('time_bonus', 0.5))
    show_correct_answer_on_wrong = quiz_status.get('show_correct_answer_on_wrong', '0') == '1'

    players_json = quiz_status.get('players', '[]')
    try:
        players = json.loads(players_json)
        participant_ids = {p['id'] for p in players if 'id' in p}

        if request.user_id not in participant_ids:
            # **السماح بالانضمام بعد بدء المسابقة**
            current_participant_count = len(participant_ids)
            if current_participant_count < max_players:
                logger.info(f"API: المستخدم {request.user_id} ينضم إلى المسابقة {quiz_unique_id} أثناء اللعب. المشاركون الحاليون: {current_participant_count}/{max_players}.")
                new_player = {
                    "id": request.user_id,
                    "username": request.username,
                    "joined_at": datetime.now().isoformat(),
                    "quiz_identifier": quiz_unique_id,
                    "bot_token": request.bot_token
                }
                players.append(new_player)
                await redis_handler.redis_client.hset(quiz_key, "players", json.dumps(players))
                await redis_handler.redis_client.hset(quiz_key, "participant_count", len(players))
                logger.info(f"API: المستخدم {request.user_id} انضم بنجاح إلى المسابقة {quiz_unique_id}. العدد الجديد: {len(players)}.")
            else:
                logger.warning(f"API: حاول المستخدم {request.user_id} الانضمام إلى المسابقة {quiz_unique_id} لكن الحد الأقصى للاعبين ({max_players}) قد وصل. رفض الإجابة.")
                raise HTTPException(status_code=403, detail=f"المسابقة ممتلئة حاليًا ({max_players} مشارك). لا يمكن الانضمام أو الإجابة.")

    except json.JSONDecodeError:
        logger.error(f"API: فشل فك ترميز JSON للاعبين للمسابقة {quiz_unique_id}: {players_json}. رفض الإجابة بسبب فساد البيانات.")
        raise HTTPException(status_code=500, detail="خطأ داخلي: فشل في معالجة قائمة المشاركين.")

    quiz_time_key = redis_handler.quiz_time_key(request.bot_token, quiz_unique_id)
    quiz_time = await redis_handler.redis_client.hgetall(quiz_time_key)

    current_question_id_in_redis = int(quiz_time.get("question_id", -1)) if quiz_time and quiz_time.get("question_id") else -1
    if current_question_id_in_redis != request.question_id:
        logger.warning(f"API: المستخدم {request.user_id} قدم إجابة للسؤال {request.question_id}، لكن السؤال النشط الحالي هو {current_question_id_in_redis}. أو وقت المسابقة مفقود.")
        raise HTTPException(status_code=400, detail="هذا ليس السؤال النشط الحالي أو السؤال قد انتهى وقته.")

    if await redis_handler.has_answered(request.bot_token, quiz_unique_id, request.question_id, request.user_id):
        logger.warning(f"API: المستخدم {request.user_id} أجاب على السؤال {request.question_id} بالفعل.")
        raise HTTPException(status_code=400, detail="لقد أجبت على هذا السؤال بالفعل.")

    questions_db_path = quiz_status.get("questions_db_path")
    if not questions_db_path:
        logger.error(f"API: مسار قاعدة بيانات الأسئلة غير موجود في حالة المسابقة لـ {request.bot_token}:{quiz_unique_id}")
        raise HTTPException(status_code=500, detail="خطأ في تهيئة المسابقة: مسار قاعدة بيانات الأسئلة مفقود.")

    question = await sqlite_handler.get_question_by_id(questions_db_path, request.question_id)
    if not question:
        logger.error(f"API: السؤال {request.question_id} غير موجود في قاعدة البيانات {questions_db_path}.")
        raise HTTPException(status_code=404, detail="لم يتم العثور على السؤال.")

    correct_option_index = question['correct_opt']
    correct_answer_text = question[f"opt{correct_option_index + 1}"]

    correct = (request.answer_index == correct_option_index)
    score_earned = 0.0

    # حساب الوقت المستغرق
    question_start_time_str = quiz_time.get("start")
    if question_start_time_str:
        question_start_time = datetime.fromisoformat(question_start_time_str)
        time_taken = (datetime.now() - question_start_time).total_seconds()
        time_taken = max(0.0, time_taken) # التأكد من أن الوقت المستغرق ليس سلبياً
    else:
        time_taken = 0.0

    if correct:
        score_earned = base_score_per_question
        # إضافة نقاط مكافأة السرعة
        if time_per_question > 0:
            speed_factor = max(0.0, 1.0 - (time_taken / time_per_question)) # عامل السرعة: 1.0 للأسرع، 0.0 للأبطأ
            score_earned += (time_bonus_per_question * speed_factor)
        logger.info(f"API: المستخدم {request.user_id} أجاب بشكل صحيح على السؤال {request.question_id}. الأساس: {base_score_per_question:.2f}، مكافأة الوقت: {score_earned - base_score_per_question:.2f}، الإجمالي: {score_earned:.2f} (الوقت: {time_taken:.2f} ثانية).")
    else:
        logger.info(f"API: المستخدم {request.user_id} أجاب بشكل خاطئ على السؤال {request.question_id}.")
        # لا توجد نقاط للإجابات الخاطئة

    # تسجيل الإجابة وتحديث نقاط المستخدم في Redis
    await redis_handler.record_answer(
        bot_token=request.bot_token,
        quiz_unique_id=quiz_unique_id,
        question_id=request.question_id,
        user_id=request.user_id,
        username=request.username,
        correct=correct,
        score_earned=score_earned,
        time_taken=time_taken
    )

    # تحديد ما إذا كان يجب إرجاع الإجابة الصحيحة في حالة الإجابة الخاطئة
    if not correct and not show_correct_answer_on_wrong:
        correct_answer_text = None # لا ترجع الإجابة الصحيحة إذا كان الإعداد معطلاً

    return {"message": "تم تقديم الإجابة.", "correct": correct, "score": round(score_earned, 2), "correct_answer_text": correct_answer_text}


@router.get("/competition_status", response_model=quiz_models.CompetitionStatusResponse)
async def competition_status(bot_token: str, quiz_identifier: str):
    logger.debug(f"API: التحقق من حالة المسابقة للبوت {bot_token} بمعرف {quiz_identifier}")
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
            logger.warning(f"API: تنسيق end_time غير صالح في Redis للمسابقة {bot_token}:{quiz_identifier}")

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
    logger.warning(f"API: طلب تنظيف يدوي للبوت {request.bot_token} بمعرف {request.quiz_identifier}.")
    await redis_handler.end_quiz(request.bot_token, request.quiz_identifier)
    return {"message": "تم تنظيف حالة Redis. ملاحظة: يتم التعامل مع حساب النتائج الكاملة وأرشفة SQLite بواسطة عامل المعالجة (worker)."}