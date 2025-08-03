import redis.asyncio as redis
import json
import os
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1") # تأكد من أن هذا يتطابق مع إعدادات Redis الخاصة بك
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(0) # قاعدة بيانات Redis الافتراضية

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

def quiz_key(bot_token: str, quiz_unique_id: str) -> str:
    return f"Quiz:{bot_token}:{quiz_unique_id}"

def quiz_answers_key(bot_token: str, quiz_unique_id: str, user_id: int) -> str:
    return f"QuizAnswers:{bot_token}:{quiz_unique_id}:{user_id}"

# هذا المفتاح لا يتم استخدامه لتخزين النتائج النهائية، بل يتم استخدام SQLite
def quiz_results_key(bot_token: str, quiz_unique_id: str) -> str:
    return f"QuizResults:{bot_token}:{quiz_unique_id}"

def quiz_time_key(bot_token: str, quiz_unique_id: str) -> str:
    return f"QuizTime:{bot_token}:{quiz_unique_id}"

# يستخدم لتتبع ما إذا كان المستخدم قد أجاب على سؤال معين بالفعل
def answered_key(bot_token: str, quiz_unique_id: str, question_id: int, user_id: int) -> str:
    return f"Answered:{bot_token}:{quiz_unique_id}:{question_id}:{user_id}"


async def start_quiz(bot_token: str, quiz_unique_id: str, questions_db_path: str, stats_db_path: str, question_ids: list, time_per_question: int, creator_id: int, initial_participant_count: int = 0):
    """
    يبدأ مسابقة جديدة في Redis.
    initial_participant_count هو عدد المشاركين في قائمة الانتظار قبل بدء المسابقة.
    """
    key = quiz_key(bot_token, quiz_unique_id)
    quiz_data = {
        "status": "initializing", # سيتم تغييره إلى "active" بواسطة API بعد إرسال السؤال الأول
        "quiz_identifier": quiz_unique_id,
        "question_ids": json.dumps(question_ids),
        "current_index": -1, # سيتم تعيينه إلى 0 بواسطة API عند بدء السؤال الأول
        "time_per_question": time_per_question,
        "start_time": datetime.now().isoformat(),
        "creator_id": creator_id,
        "bot_token": bot_token,
        "questions_db_path": questions_db_path,
        "stats_db_path": stats_db_path,
        "participant_count": initial_participant_count # **التحسين: إضافة عداد المشاركين الأولي**
    }
    await redis_client.hset(key, mapping=quiz_data) # استخدام mapping لتمرير القاموس بالكامل
    logger.info(f"Redis: Quiz {key} initialized with status 'initializing' and {initial_participant_count} participants.")

async def get_quiz_status(bot_token: str, quiz_unique_id: str):
    """يسترجع حالة مسابقة معينة من Redis."""
    # ملاحظة: هذه الدالة لم تعد تُستخدم مباشرة في quiz_api.py، بل get_quiz_status_by_key
    key = quiz_key(bot_token, quiz_unique_id)
    return await redis_client.hgetall(key)

async def get_quiz_status_by_key(key: str):
    """يسترجع حالة مسابقة معينة باستخدام مفتاحها الكامل."""
    return await redis_client.hgetall(key)

async def set_current_question(bot_token: str, quiz_unique_id: str, question_id: int, end_time: datetime):
    """
    يسجل السؤال النشط الحالي ووقته النهائي.
    يتم تعيين صلاحية لهذا المفتاح + 5 ثوانٍ بعد انتهاء وقت السؤال لتغطية أي تأخير.
    """
    key = quiz_time_key(bot_token, quiz_unique_id)
    time_data = {
        "question_id": question_id,
        "start": datetime.now().isoformat(),
        "end": end_time.isoformat()
    }
    await redis_client.hset(key, mapping=time_data)
    await redis_client.expireat(key, end_time + timedelta(seconds=10)) # صلاحية لضمان تنظيف المؤقت
    logger.debug(f"Redis: Current question set to {question_id} for {quiz_time_key(bot_token, quiz_unique_id)}. Expires at {end_time.isoformat()}.")


async def has_answered(bot_token: str, quiz_unique_id: str, question_id: int, user_id: int) -> bool:
    """يتحقق مما إذا كان المستخدم قد أجاب على سؤال معين."""
    key = answered_key(bot_token, quiz_unique_id, question_id, user_id)
    return await redis_client.exists(key)

async def record_answer(bot_token: str, quiz_unique_id: str, question_id: int, user_id: int, username: str, score: int, time_per_question: int):
    """
    يسجل إجابة المستخدم ويزيد نقاطه.
    لم تعد ترجع `is_new_participant_in_quiz` لأن هذا يتم إدارته من جانب PHP عند الانضمام.
    """
    answered_key_str = answered_key(bot_token, quiz_unique_id, question_id, user_id)
    answers_key = quiz_answers_key(bot_token, quiz_unique_id, user_id)

    async with redis_client.pipeline() as pipe:
        # تعيين مفتاح Answered:{...} بصلاحية لمنع الإجابات المتعددة على نفس السؤال
        pipe.setex(answered_key_str, time_per_question + 10, "true") # صلاحية أكثر قليلاً من وقت السؤال
        # تحديث اسم المستخدم (قد يتغير) والنقاط لكل إجابة في Hash الخاص باللاعب
        pipe.hset(answers_key, "username", username)
        pipe.hincrby(answers_key, "score", score)
        # تسجيل الإجابة على سؤال معين (لتتبع الإجابات الفردية في النهاية)
        pipe.hset(answers_key, f"answers.{question_id}", score)
        await pipe.execute()

    logger.debug(f"Redis: Recorded answer for user {user_id} on Q{question_id} (score: {score}) in quiz {quiz_unique_id}.")

    # لم نعد نرجع True/False هنا، لأن المنطق يعتمد الآن على قائمة `players` في Redis.
    return None

async def end_quiz(bot_token: str, quiz_unique_id: str):
    """
    تقوم بمسح جميع بيانات مسابقة معينة من Redis.
    """
    logger.info(f"Redis: Initiating full Redis cleanup for bot {bot_token}, quiz {quiz_unique_id}.")

    keys_to_delete_list = []

    # المفاتيح الرئيسية للمسابقة وحالتها الزمنية وقفل الإنهاء
    main_quiz_key = quiz_key(bot_token, quiz_unique_id)
    quiz_time_key_str = quiz_time_key(bot_token, quiz_unique_id)
    end_quiz_lock_key = f"Lock:EndQuiz:{main_quiz_key}"
    processing_lock_key = f"Lock:Process:{main_quiz_key}" # تأكد من حذف قفل المعالجة أيضاً

    keys_to_delete_list.extend([main_quiz_key, quiz_time_key_str, end_quiz_lock_key, processing_lock_key])

    # مفاتيح الإجابات الفردية للمشاركين
    async for key in redis_client.scan_iter(f"QuizAnswers:{bot_token}:{quiz_unique_id}:*"):
        keys_to_delete_list.append(key)
    # مفاتيح تتبع إذا كان المستخدم قد أجاب على سؤال معين
    async for key in redis_client.scan_iter(f"Answered:{bot_token}:{quiz_unique_id}:*:*"):
        keys_to_delete_list.append(key)

    keys_to_delete_list = [k for k in keys_to_delete_list if k is not None]

    if keys_to_delete_list:
        deleted_count = await redis_client.delete(*keys_to_delete_list)
        logger.info(f"Redis: Cleanup complete for bot {bot_token}, quiz {quiz_unique_id}. Deleted {deleted_count} keys.")
    else:
        logger.info(f"Redis: No specific Redis keys found for cleanup for bot {bot_token}, quiz {quiz_unique_id}.")