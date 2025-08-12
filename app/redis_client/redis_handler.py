import redis.asyncio as redis
import json
import os
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(0)

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def quiz_key(bot_token: str, quiz_unique_id: str) -> str:
    return f"Quiz:{bot_token}:{quiz_unique_id}"

def quiz_answers_key(bot_token: str, quiz_unique_id: str, user_id: int) -> str:
    return f"QuizAnswers:{bot_token}:{quiz_unique_id}:{user_id}"

def quiz_time_key(bot_token: str, quiz_unique_id: str) -> str:
    return f"QuizTime:{bot_token}:{quiz_unique_id}"

def answered_key(bot_token: str, quiz_unique_id: str, question_id: int, user_id: int) -> str:
    return f"Answered:{bot_token}:{quiz_unique_id}:{question_id}:{user_id}"

async def start_quiz(
        bot_token: str,
        quiz_unique_id: str,
        questions_db_path: str,
        stats_db_path: str,
        question_ids: list,
        time_per_question: int,
        creator_id: int,
        initial_participant_count: int,
        max_players: int,
        eliminate_after: Optional[int], # -1 if None or 0
        base_score: float,
        time_bonus: float,
        show_correct_answer_on_wrong: bool,
        chat_id: Optional[int] # معلمة جديدة: معرف المحادثة
):
    key = quiz_key(bot_token, quiz_unique_id)
    existing_data = await redis_client.hgetall(key)

    quiz_data = {
        "status": "initializing",
        "quiz_identifier": quiz_unique_id,
        "question_ids": json.dumps(question_ids),
        "current_index": -1,
        "time_per_question": time_per_question,
        "start_time": datetime.now().isoformat(),
        "creator_id": creator_id,
        "bot_token": bot_token,
        "questions_db_path": questions_db_path,
        "stats_db_path": stats_db_path,
        "participant_count": initial_participant_count,
        "max_players": max_players,
        "eliminate_after": eliminate_after if eliminate_after is not None else -1, # -1 لتعطيله
        "base_score": base_score,
        "time_bonus": time_bonus,
        "show_correct_answer_on_wrong": "1" if show_correct_answer_on_wrong else "0",
        "chat_id": str(chat_id) if chat_id is not None else "", # تخزين معرف المحادثة كسلسلة أو فارغ
    }

    merged_data = {**existing_data, **quiz_data}
    # التأكد من عدم ترحيل معلومات الوقت من حالة سابقة
    merged_data.pop("current_question_end_time", None)
    merged_data.pop("round_results_displayed_at", None)

    await redis_client.hset(key, mapping=merged_data)
    await redis_client.expire(key, timedelta(hours=24))

    quiz_time_key_str = quiz_time_key(bot_token, quiz_unique_id)
    await redis_client.delete(quiz_time_key_str)
    logger.info(f"Redis: Quiz {key} initialized with new settings.")

async def get_quiz_status_by_key(key: str) -> Dict[str, str]:
    return await redis_client.hgetall(key)

async def set_current_question(bot_token: str, quiz_unique_id: str, question_id: int, end_time: datetime):
    key = quiz_time_key(bot_token, quiz_unique_id)
    time_data = {
        "question_id": question_id,
        "start": datetime.now().isoformat(),
        "end": end_time.isoformat()
    }
    await redis_client.hset(key, mapping=time_data)
    # تاريخ انتهاء الصلاحية يجب أن يكون بعد انتهاء السؤال بمدة قصيرة للسماح للعامل بالمعالجة
    await redis_client.expireat(key, end_time + timedelta(seconds=15))
    logger.debug(f"Redis: Current question set to {question_id} for quiz {quiz_unique_id}.")

async def has_answered(bot_token: str, quiz_unique_id: str, question_id: int, user_id: int) -> bool:
    key = answered_key(bot_token, quiz_unique_id, question_id, user_id)
    return await redis_client.exists(key)

async def record_answer(
        bot_token: str,
        quiz_unique_id: str,
        question_id: int,
        user_id: int,
        username: str,
        correct: bool,
        score_earned: float, # الآن يمكن أن تكون النقاط عشرية
        time_taken: float
):
    answered_key_str = answered_key(bot_token, quiz_unique_id, question_id, user_id)
    answers_key = quiz_answers_key(bot_token, quiz_unique_id, user_id)

    # الحصول على time_per_question لتعيين صلاحية المفتاح
    quiz_details = await redis_client.hgetall(quiz_key(bot_token, quiz_unique_id))
    time_per_question = int(quiz_details.get("time_per_question", 30))

    async with redis_client.pipeline() as pipe:
        # تعيين مفتاح Answered:{...} بصلاحية لمنع الإجابات المتعددة على نفس السؤال
        await pipe.setex(answered_key_str, time_per_question + 10, "true")

        # تحديث اسم المستخدم (قد يتغير)
        await pipe.hset(answers_key, "username", username)

        # تخزين بيانات الإجابة التفصيلية لهذا السؤال
        answer_data = {
            "correct": correct,
            "score": round(score_earned, 2),
            "time": round(time_taken, 2),
            "question_id": question_id # التأكد من أن معرف السؤال جزء من البيانات المخزنة
        }
        await pipe.hset(answers_key, f"q_data:{question_id}", json.dumps(answer_data))

        # تحديث المجاميع الإجمالية
        await pipe.hincrbyfloat(answers_key, "total_score", score_earned)
        if correct:
            await pipe.hincrby(answers_key, "correct_answers_count", 1)
        else:
            await pipe.hincrby(answers_key, "wrong_answers_count", 1) # لخاصية الإقصاء

        # تعيين صلاحية لإجابات المستخدم الكلية لهذه المسابقة
        # يجب أن تتطابق مع صلاحية المسابقة الرئيسية للاتساق (24 ساعة من البداية)
        await pipe.expire(answers_key, timedelta(hours=24))

        await pipe.execute()
    logger.debug(f"Redis: تم تسجيل إجابة المستخدم {user_id} على سؤال {question_id} (نقاط: {score_earned:.2f}, وقت: {time_taken:.2f} ثانية) في المسابقة {quiz_unique_id}.")

async def get_player_answers_for_question(bot_token: str, quiz_unique_id: str, question_id: int) -> Dict[int, Dict[str, Any]]:
    """
    يسترد الإجابات المقدمة من جميع اللاعبين لسؤال معين.
    المرتجع: {user_id: {"username": "...", "correct": bool, "score": float, "time": float, "wrong_answers_total": int}}
    """
    answers = {}
    async for key in redis_client.scan_iter(f"QuizAnswers:{bot_token}:{quiz_unique_id}:*"):
        user_id = int(key.split(":")[-1])
        user_data = await redis_client.hgetall(key)
        username = user_data.get("username", f"مستخدم {user_id}")

        question_data_str = user_data.get(f"q_data:{question_id}")
        if question_data_str:
            try:
                q_data = json.loads(question_data_str)
                answers[user_id] = {
                    "username": username,
                    "correct": q_data.get("correct", False),
                    "score": q_data.get("score", 0.0),
                    "time": q_data.get("time", 0.0),
                    "wrong_answers_total": int(user_data.get("wrong_answers_count", 0)) # تضمين إجمالي الإجابات الخاطئة للتحقق من الإقصاء
                }
            except json.JSONDecodeError:
                logger.warning(f"Redis: لم يتمكن من فك ترميز بيانات الإجابة للمستخدم {user_id}، سؤال {question_id} في المسابقة {quiz_unique_id}.")
    return answers


async def end_quiz(bot_token: str, quiz_unique_id: str):
    logger.info(f"Redis: بدء تنظيف Redis الكامل للمسابقة {bot_token}, {quiz_unique_id}.")
    keys_to_delete_list = []
    main_quiz_key = quiz_key(bot_token, quiz_unique_id)
    keys_to_delete_list.append(main_quiz_key)

    # حذف الأقفال والمؤقتات
    keys_to_delete_list.append(quiz_time_key(bot_token, quiz_unique_id))
    keys_to_delete_list.append(f"Lock:EndQuiz:{main_quiz_key}")
    keys_to_delete_list.append(f"Lock:Process:{main_quiz_key}")

    # حذف بيانات إجابات اللاعبين
    async for key in redis_client.scan_iter(f"QuizAnswers:{bot_token}:{quiz_unique_id}:*"):
        keys_to_delete_list.append(key)
    # حذف علامات الإجابة
    async for key in redis_client.scan_iter(f"Answered:{bot_token}:{quiz_unique_id}:*:*"):
        keys_to_delete_list.append(key)

    keys_to_delete_list = [k for k in keys_to_delete_list if k]
    if keys_to_delete_list:
        deleted_count = await redis_client.delete(*keys_to_delete_list)
        logger.info(f"Redis: اكتمل التنظيف. تم حذف {deleted_count} مفتاح للمسابقة {quiz_unique_id}.")
    else:
        logger.info(f"Redis: لم يتم العثور على مفاتيح Redis محددة لتنظيفها للمسابقة {bot_token}, {quiz_unique_id}.")