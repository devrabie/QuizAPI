import asyncio
import json
from datetime import datetime, timedelta
import logging
import os
import html

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from app.redis_client import redis_handler
    from app.database import sqlite_handler
    from app.services.telegram_bot import TelegramBotServiceAsync
except ImportError:
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from app.redis_client import redis_handler
    from app.database import sqlite_handler
    from app.services.telegram_bot import TelegramBotServiceAsync

bot_instances = {}

# **التحسين: قاموس لتتبع معدل الطلبات لكل بوت باستخدام Token Bucket / Leaky Bucket**
# هذا سيساعد في التعامل مع "Too Many Requests"
# لكل بوت: { "last_call_time": datetime, "tokens": float, "last_retry_after": datetime }
bot_rate_limiter = {}
RATE_LIMIT_TOKENS_PER_SECOND = 1.0 # مثال: 1 توكن/ثانية لكل بوت. يمكن زيادته إلى 2-3 لـ editMessage (حوالي 60 طلب/دقيقة)
RATE_LIMIT_BUCKET_SIZE = 5.0      # حجم الجردل الأقصى (عدد الطلبات التي يمكن تخزينها فجأة)

# تعريف علامات HTML لطي النص
BLOCKQUOTE_OPEN_TAG = "<blockquote expandable>"
BLOCKQUOTE_CLOSE_TAG = "</blockquote>"

def get_telegram_bot(token: str) -> TelegramBotServiceAsync:
    if token not in bot_instances:
        bot_instances[token] = TelegramBotServiceAsync(token)
    return bot_instances[token]

async def _is_api_call_allowed(bot_token: str, wait_for_tokens: bool = False) -> bool:
    """
    آلية متقدمة لتنظيم الطلبات لكل بوت باستخدام نموذج Token Bucket.
    تحسب التوكنات المتاحة وتطبق تأخيرًا إذا لزم الأمر.
    """
    now = datetime.now()
    if bot_token not in bot_rate_limiter:
        bot_rate_limiter[bot_token] = {
            "last_call_time": now,
            "tokens": RATE_LIMIT_BUCKET_SIZE, # نبدأ بالجردل ممتلئًا
            "last_retry_after": now # آخر وقت طلب فيه تيليجرام الانتظار
        }

    bucket = bot_rate_limiter[bot_token]

    # إذا طلب تيليجرام منا الانتظار، نلتزم بذلك
    if (now - bucket["last_retry_after"]).total_seconds() < 0: # هذا يعني أن هناك قيمة في المستقبل
        return False

    # إضافة توكنات للجردل بناءً على الوقت المنقضي
    time_passed = (now - bucket["last_call_time"]).total_seconds()
    bucket["tokens"] = min(RATE_LIMIT_BUCKET_SIZE, bucket["tokens"] + time_passed * RATE_LIMIT_TOKENS_PER_SECOND)
    bucket["last_call_time"] = now

    if bucket["tokens"] >= 1.0: # نحتاج توكن واحد لكل طلب
        bucket["tokens"] -= 1.0
        return True
    elif wait_for_tokens:
        # إذا سمحنا بالانتظار، نحسب كم نحتاج للانتظار
        wait_time = (1.0 - bucket["tokens"]) / RATE_LIMIT_TOKENS_PER_SECOND
        logger.debug(f"Worker: Rate limit for bot {bot_token}. Waiting {wait_time:.2f}s for tokens.")
        await asyncio.sleep(wait_time)
        bucket["tokens"] = 0.0 # استنفذنا التوكن بعد الانتظار
        return True
    else:
        return False

async def _send_telegram_update(quiz_key: str, telegram_bot: TelegramBotServiceAsync, message_data: dict, quiz_status: dict):
    """
    دالة مساعدة مركزية لإرسال تحديثات الرسائل إلى تيليجرام.
    تتعامل مع كل من الرسائل العادية ورسائل Inline.
    تتعامل مع أخطاء API وتحديث حالة المسابقة.
    """
    bot_token = quiz_status.get("bot_token")
    if not await _is_api_call_allowed(bot_token, wait_for_tokens=True): # حاول الحصول على توكن، انتظر إذا لزم الأمر
        logger.debug(f"Worker: [{quiz_key}] Telegram update skipped/delayed due to rate limit for bot {bot_token}.")
        return

    inline_message_id = quiz_status.get("inline_message_id")
    chat_id = quiz_status.get("chat_id")
    message_id = quiz_status.get("message_id")
    now = datetime.now()

    try:
        response = None
        # تحديد ما إذا كانت الرسالة inline أو رسالة عادية
        if inline_message_id:
            message_data["inline_message_id"] = inline_message_id
            response = await asyncio.wait_for(telegram_bot.edit_inline_message(message_data), timeout=10.0)
        elif chat_id and message_id:
            message_data["chat_id"] = chat_id
            message_data["message_id"] = message_id
            response = await asyncio.wait_for(telegram_bot.edit_message(message_data), timeout=10.0)
        else:
            logger.error(f"Worker: [{quiz_key}] No valid message identifier (inline_message_id OR chat_id/message_id) for editing. Cannot send update.")
            # إذا لم يكن هناك معرف رسالة، نضع المسابقة في حالة "stopping"
            await redis_handler.redis_client.hset(quiz_key, "status", "stopping")
            return

        if not response.get("ok"):
            desc = response.get("description", "")
            if "message is not modified" not in desc:
                logger.error(f"Worker: [{quiz_key}] Telegram reported failure to update display: {desc}")
                # **التحسين: التعامل مع الأخطاء الحرجة**
                # إذا كانت الرسالة غير صالحة أو تم حظر البوت، نوقف المسابقة
                if "MESSAGE_ID_INVALID" in desc or "bot was blocked by the user" in desc or "chat not found" in desc:
                    logger.warning(f"Worker: [{quiz_key}] Critical Telegram error ({desc}). Setting quiz to 'stopping'.")
                    await redis_handler.redis_client.hset(quiz_key, "status", "stopping")
                elif "Too Many Requests" in desc:
                    retry_after = response.get("parameters", {}).get("retry_after", 5) # استخراج وقت الانتظار
                    logger.warning(f"Worker: [{quiz_key}] Too Many Requests for bot {bot_token}. Retrying after {retry_after}s.")
                    # تحديث وقت الانتظار في Rate Limiter للجردل هذا البوت
                    bot_rate_limiter[bot_token]["last_retry_after"] = now + timedelta(seconds=retry_after)
                    # يجب أن لا يحاول البوت إرسال طلبات لنفس البوت حتى ينتهي وقت الانتظار
        else:
            logger.debug(f"Worker: [{quiz_key}] Successfully updated display message.")
            # إعادة ضبط وقت last_retry_after عند النجاح
            if bot_token in bot_rate_limiter:
                bot_rate_limiter[bot_token]["last_retry_after"] = now # أو now - timedelta(seconds=1)

    except asyncio.TimeoutError:
        logger.warning(f"Worker: [{quiz_key}] Timed out while trying to send Telegram update.")
    except Exception as e:
        logger.error(f"Worker: [{quiz_key}] Failed to send Telegram update due to an exception: {e}", exc_info=True)


async def update_pending_display(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync, force_update: bool = False):
    """
    دالة لتحديث رسالة حالة المسابقة عندما تكون في حالة "pending" (انتظار اللاعبين).
    """
    UPDATE_INTERVAL_SECONDS = 5
    now = datetime.now()

    if not force_update:
        last_update_str = await redis_handler.redis_client.hget(quiz_key, "last_display_update")
        if last_update_str:
            try:
                if (now - datetime.fromisoformat(last_update_str)).total_seconds() < UPDATE_INTERVAL_SECONDS:
                    return
            except (ValueError, TypeError):
                logger.warning(f"Worker: [{quiz_key}] Could not parse last_display_update timestamp: {last_update_str}")

    bot_token = quiz_status.get("bot_token")
    if not await _is_api_call_allowed(bot_token): # لا ننتظر هنا، نكتشف لاحقاً إذا كان الطلب بطيئاً
        logger.debug(f"Worker: [{quiz_key}] Pending display update skipped due to GLOBAL rate limit for bot {bot_token}.")
        return

    await redis_handler.redis_client.hset(quiz_key, "last_display_update", now.isoformat())
    logger.info(f"Worker: [{quiz_key}] Proceeding with pending display update (force_update={force_update}).")

    players_json = quiz_status.get('players', '[]')
    try:
        players = json.loads(players_json)
    except json.JSONDecodeError:
        logger.warning(f"Worker: [{quiz_key}] Could not decode 'players' JSON: {players_json}. Assuming no players.")
        players = []

    players_count = len(players)
    creator_name = html.escape(quiz_status.get('creator_username', 'غير معروف'))
    quiz_type = html.escape(quiz_status.get('quiz_type', 'عامة'))
    quiz_game_id = quiz_status.get('quiz_identifier', 'N/A')
    creator_user_id = quiz_status.get('creator_id')
    max_players = int(quiz_status.get("max_players", 12))


    players_list_str = "\n".join([f"{i+1}- {html.escape(p.get('username', 'مجهول'))}" for i, p in enumerate(players[:10])])
    if not players_list_str:
        players_list_str = "لا يوجد لاعبون بعد."

    message_text = (
        f"🎮 <b>مسابقة أسئلة جديدة!</b>\n\n"
        f"🎯 <b>الفئة</b>: {quiz_type}\n"
        f"👤 <b>المنشئ</b>: {creator_name}\n\n"
        f"👥 <b>اللاعبون ({players_count}/{max_players}):</b>\n{players_list_str}"
    )

    buttons = {
        "inline_keyboard": [
            [{"text": '➡️ انضم للمسابقة', "callback_data": f"quiz_join|{quiz_game_id}|{creator_user_id}"}],
            [{"text": '▶️ ابدأ المسابقة', "callback_data": f"quiz_start|{quiz_game_id}|{creator_user_id}"}]
        ]
    }

    # تعطيل زر الانضمام إذا وصل العدد الأقصى
    if players_count >= max_players:
        for row_idx, row in enumerate(buttons["inline_keyboard"]):
            for btn_idx, button in enumerate(row):
                if button.get("callback_data") and button["callback_data"].startswith("quiz_join"):
                    buttons["inline_keyboard"][row_idx][btn_idx] = {"text": '👥 العدد مكتمل', "callback_data": 'ignore_full_quiz'}
                    break
            else:
                continue
            break

    message_data = {
        "text": message_text,
        "reply_markup": json.dumps(buttons),
        "parse_mode": "HTML"
    }

    await _send_telegram_update(quiz_key, telegram_bot, message_data, quiz_status)


async def update_question_display(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync, time_left: float, force_update: bool = False):
    """
    دالة لتحديث رسالة عرض السؤال النشط في المسابقة.
    """
    UPDATE_INTERVAL_SECONDS = 4
    now = datetime.now()

    if not force_update:
        last_update_str = await redis_handler.redis_client.hget(quiz_key, "last_display_update")
        if last_update_str:
            try:
                if (now - datetime.fromisoformat(last_update_str)).total_seconds() < UPDATE_INTERVAL_SECONDS:
                    return
            except (ValueError, TypeError):
                logger.warning(f"Worker: [{quiz_key}] Could not parse last_display_update timestamp: {last_update_str}")

    bot_token = quiz_status.get("bot_token")
    if not await _is_api_call_allowed(bot_token):
        logger.debug(f"Worker: [{quiz_key}] Active display update skipped due to GLOBAL rate limit for bot {bot_token}.")
        return

    await redis_handler.redis_client.hset(quiz_key, "last_display_update", now.isoformat())
    logger.info(f"Worker: [{quiz_key}] Proceeding with active display update (force_update={force_update}).")

    category_display_name = quiz_status.get("category_display_name", "عامة")
    base_question_text_from_redis = quiz_status.get("current_question_text", "")
    participants = int(quiz_status.get("participant_count", 0))

    if not base_question_text_from_redis:
        return

    new_text = (
        f"❓ {base_question_text_from_redis}\n\n"
        f"🏷️ <b>الفئة</b>: {html.escape(category_display_name)}\n"
        f"👥 <b>المشاركون</b>: {participants}\n"
        f"⏳ <b>الوقت المتبقي</b>: {int(time_left)} ثانية"
    )

    current_keyboard_str = quiz_status.get("current_keyboard")
    if not current_keyboard_str:
        return

    current_keyboard = json.loads(current_keyboard_str)

    additional_buttons = []
    quiz_identifier = quiz_status.get("quiz_identifier")
    creator_user_id = quiz_status.get("creator_id")
    # additional_buttons.append([{"text": '➡️ لوحة المتصدرين', "callback_data": f"show_leaderboard|{quiz_identifier}|{creator_user_id}"}])

    updated_keyboard = {"inline_keyboard": current_keyboard["inline_keyboard"] + additional_buttons}

    message_data = {
        "text": new_text,
        "reply_markup": json.dumps(updated_keyboard),
        "parse_mode": "HTML"
    }

    await _send_telegram_update(quiz_key, telegram_bot, message_data, quiz_status)


async def process_active_quiz(quiz_key: str):
    processing_lock_key = f"Lock:Process:{quiz_key}"
    if not await redis_handler.redis_client.set(processing_lock_key, "true", ex=10, nx=True):
        logger.debug(f"Worker: [{quiz_key}] Processing is already locked. Skipping to prevent race conditions.")
        return

    try:
        logger.debug(f"Worker: Processing quiz key: {quiz_key} (Lock acquired).")
        quiz_status = await redis_handler.get_quiz_status_by_key(quiz_key)

        if not quiz_status:
            logger.warning(f"Worker: [{quiz_key}] No status found in Redis. It might have been cleaned up. Skipping.")
            return

        bot_token = quiz_status.get("bot_token")
        quiz_identifier = quiz_status.get("quiz_identifier")

        if not bot_token or not quiz_identifier:
            logger.error(f"Worker: [{quiz_key}] Bot token or quiz_identifier missing. Cleaning up broken state.")
            await redis_handler.redis_client.delete(quiz_key)
            return

        telegram_bot = get_telegram_bot(bot_token)
        status = quiz_status.get("status")

        if status == "stopping":
            logger.info(f"Worker: [{quiz_key}] Found in 'stopping' state. Attempting to finalize.")
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        if status == "pending":
            logger.debug(f"Worker: [{quiz_key}] Status is 'pending'. Updating display for waiting players.")
            await update_pending_display(quiz_key, quiz_status, telegram_bot)
            return

        if status not in ["active"]:
            logger.debug(f"Worker: [{quiz_key}] Status is '{status}'. Skipping question progression.")
            return

        quiz_time_key = redis_handler.quiz_time_key(bot_token, quiz_identifier)
        quiz_time = await redis_handler.redis_client.hgetall(quiz_time_key)

        should_process_next_question = False
        if quiz_time and "end" in quiz_time:
            try:
                end_time = datetime.fromisoformat(quiz_time["end"])
                if datetime.now() >= end_time:
                    logger.info(f"Worker: [{quiz_key}] Question timer has expired. Proceeding to next question.")
                    should_process_next_question = True
                else:
                    time_left = (end_time - datetime.now()).total_seconds()
                    logger.debug(f"Worker: [{quiz_key}] Timer active. {time_left:.1f}s left. Calling display updater.")
                    await update_question_display(quiz_key, quiz_status, telegram_bot, time_left)
                    return
            except (ValueError, TypeError):
                logger.error(f"Worker: [{quiz_key}] Invalid end_time format: {quiz_time.get('end')}. Forcing next question.")
                should_process_next_question = True
        else:
            logger.info(f"Worker: [{quiz_key}] No active question timer found. Assuming it's time for the next question (or initial question).")
            should_process_next_question = True

        if should_process_next_question:
            await handle_next_question(quiz_key, quiz_status, telegram_bot)

    finally:
        await redis_handler.redis_client.delete(processing_lock_key)
        logger.debug(f"Worker: [{quiz_key}] Processing lock released.")


async def handle_next_question(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    """
    تُعالج انتقال المسابقة إلى السؤال التالي أو نهايتها.
    """
    current_index = int(quiz_status.get("current_index", -1))
    question_ids_str = quiz_status.get("question_ids", "[]")

    try:
        question_ids = json.loads(question_ids_str)
    except json.JSONDecodeError:
        logger.error(f"Worker: [{quiz_key}] Failed to decode question_ids JSON string: {question_ids_str}. Ending quiz.")
        await end_quiz(quiz_key, quiz_status, telegram_bot)
        return

    next_index = current_index + 1

    logger.info(f"Worker: [{quiz_key}] Handling next question logic. Current Index: {current_index}, Next Index: {next_index}, Total Qs: {len(question_ids)}")

    if next_index < len(question_ids):
        next_question_id = question_ids[next_index]

        questions_db_path = quiz_status.get("questions_db_path")

        if not questions_db_path:
            logger.error(f"Worker: [{quiz_key}] 'questions_db_path' missing. Cannot fetch question. Ending quiz.")
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        question = await sqlite_handler.get_question_by_id(questions_db_path, next_question_id)
        if not question:
            logger.error(f"Worker: [{quiz_key}] Question ID {next_question_id} not found in DB '{questions_db_path}'. Ending quiz.")
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        base_question_text_for_redis = f"<b>السؤال  {next_index + 1} </b>:\n{question['question']}"

        options = [question['opt1'], question['opt2'], question['opt3'], question['opt4']]
        quiz_identifier_for_callbacks = quiz_status.get("quiz_identifier")
        keyboard = {"inline_keyboard": [[{"text": opt, "callback_data": f"answer_{quiz_identifier_for_callbacks}_{next_question_id}_{i}"}] for i, opt in enumerate(options)]}

        time_per_question = int(quiz_status.get("time_per_question", 30))
        participants_count = int(quiz_status.get("participant_count", 0))

        category_display_name = quiz_status.get("category_display_name", "عامة")

        full_new_question_message_text = (
            f"❓ {base_question_text_for_redis}\n\n"
            f"🏷️ <b>الفئة</b>: {html.escape(category_display_name)}\n"
            f"👥 <b>المشاركون</b>: {participants_count}\n"
            f"⏳ <b>الوقت المتبقي</b>: {time_per_question} ثانية"
        )

        message_data = {
            "text": full_new_question_message_text,
            "reply_markup": json.dumps(keyboard),
            "parse_mode": "HTML"
        }

        logger.info(f"Worker: [{quiz_key}] Attempting to edit message for Q{next_index + 1} (ID: {next_question_id}).")
        await _send_telegram_update(quiz_key, telegram_bot, message_data, quiz_status)

        end_time = datetime.now() + timedelta(seconds=time_per_question)

        bot_token = quiz_status.get("bot_token")
        quiz_identifier = quiz_status.get("quiz_identifier")
        await redis_handler.set_current_question(bot_token, quiz_identifier, next_question_id, end_time)
        await redis_handler.redis_client.hset(
            quiz_key, mapping={
                "current_question_text": base_question_text_for_redis,
                "current_keyboard": json.dumps(keyboard),
                "current_index": next_index
            }
        )

        logger.info(f"Worker: [{quiz_key}] State updated. New current_index: {next_index}. Timer set for {time_per_question}s.")
        refreshed_quiz_status = await redis_handler.get_quiz_status_by_key(quiz_key)
        await update_question_display(quiz_key, refreshed_quiz_status, telegram_bot, time_per_question, force_update=True)

    else:
        logger.info(f"Worker: [{quiz_key}] End of questions reached. Finishing up.")
        await end_quiz(quiz_key, quiz_status, telegram_bot)


async def end_quiz(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    """
    تُعالج إنهاء المسابقة، حساب النتائج، حفظها في SQLite، ومسح بياناتها من Redis.
    تستخدم قفلًا لضمان عدم تداخل عمليات الإنهاء.
    """
    lock_key = f"Lock:EndQuiz:{quiz_key}"
    if not await redis_handler.redis_client.set(lock_key, "true", ex=60, nx=True):
        logger.warning(f"Worker: [{quiz_key}] End process is already locked or in progress. Skipping to prevent loop.")
        return

    try:
        logger.info(f"Worker: [{quiz_key}] Starting end_quiz process (lock acquired).")

        bot_token = quiz_status.get("bot_token")
        quiz_identifier = quiz_status.get("quiz_identifier")
        stats_db_path = quiz_status.get("stats_db_path")
        # جلب نص التوقيع
        signature_text = quiz_status.get("signature_text", "بـوت تـحدي الاسئلة ❓ (https://t.me/nniirrbot)")


        if not bot_token or not quiz_identifier:
            logger.error(f"Worker: [{quiz_key}] Cannot end quiz, bot_token or quiz_identifier is missing. Releasing lock and exiting.")
            return

        # inline_message_id = quiz_status.get("inline_message_id") # لم نعد نستخدمها مباشرة هنا
        # chat_id = quiz_status.get("chat_id") # لم نعد نستخدمها مباشرة هنا
        # message_id = quiz_status.get("message_id") # لم نعد نستخدمها مباشرة هنا

        if not stats_db_path:
            results_text = "🏆 <b>المسابقة انتهت!</b> 🏆\n\nحدث خطأ في حفظ النتائج. يرجى مراجعة سجلات الخادم."
            message_data = {"text": results_text, "reply_markup": json.dumps({}), "parse_mode": "HTML"}
            await _send_telegram_update(quiz_key, telegram_bot, message_data, quiz_status)
            await redis_handler.end_quiz(bot_token, quiz_identifier)
            return

        total_questions = len(json.loads(quiz_status.get("question_ids", "[]")))

        logger.info(f"Worker: [{quiz_key}] Calculating results from Redis.")
        final_scores = {}
        # إحصائيات للمشاركين
        total_participants_who_answered = 0 # عدد المشاركين الذين قدموا إجابة واحدة على الأقل (حتى لو كانت خاطئة)

        async for key in redis_handler.redis_client.scan_iter(f"QuizAnswers:{bot_token}:{quiz_identifier}:*"):
            try:
                user_id = int(key.split(":")[-1])
                user_data = await redis_handler.redis_client.hgetall(key)
                score = int(user_data.get('score', 0))
                username = html.escape(user_data.get('username', f"User_{user_id}"))

                user_answers = {}
                for k, v in user_data.items():
                    if k.startswith('answers.'):
                        try:
                            q_id = int(k.split('.')[1])
                            user_answers[q_id] = int(v)
                        except ValueError:
                            logger.warning(f"Worker: [{quiz_key}] Malformed answer key/value for user {user_id}, key {k}: {v}")

                final_scores[user_id] = {'score': score, 'username': username, 'answers': user_answers}

                if len(user_answers) > 0: # إذا أجاب المستخدم على سؤال واحد على الأقل
                    total_participants_who_answered += 1

                logger.debug(f"Worker: [{quiz_key}] Collected results for user {user_id}: score={score}, username={username}")
            except (ValueError, IndexError) as e:
                logger.warning(f"Worker: [{quiz_key}] Could not parse user data from answer key '{key}': {e}")
                continue

        # جلب قائمة اللاعبين المسجلين من حالة المسابقة (من حقل 'players')
        registered_players_json = quiz_status.get('players', '[]')
        try:
            registered_players = json.loads(registered_players_json)
            total_registered_players = len(registered_players)
        except json.JSONDecodeError:
            logger.warning(f"Worker: [{quiz_key}] Could not decode 'players' JSON for registered participants.")
            total_registered_players = 0

        # عدد اللاعبين المسجلين الذين لم يقدموا أي إجابة
        not_answered_count = total_registered_players - total_participants_who_answered
        if not_answered_count < 0: # في حالة وجود خطأ أو بيانات غير متناسقة (يجب ألا يحدث نظريا)
            not_answered_count = 0

        sorted_participants = sorted(final_scores.items(), key=lambda item: item[1]['score'], reverse=True)
        winner_id, winner_score, winner_username_escaped = (None, 0, "لا يوجد")
        if sorted_participants:
            winner_id, winner_data = sorted_participants[0]
            winner_score, winner_username_escaped = winner_data['score'], winner_data['username']

        ltr = '\u202A'
        pdf = '\u202C'

        results_text = "🏆 <b>المسابقة انتهت! النتائج النهائية:</b> 🏆\n\n"
        if winner_id:
            results_text += f"🎉 <b>الفائز</b>: {ltr}{winner_username_escaped}{pdf} بـ {winner_score} نقطة!\n\n"
        else:
            results_text += "😞 لم يشارك أحد في المسابقة أو لم يحصل أحد على نقاط.\n\n"

        # إحصائيات المشاركة الجديدة
        results_text += f"{BLOCKQUOTE_OPEN_TAG}"
        results_text += f"📊 <b>إحصائيات المشاركة:</b>\n"
        results_text += f"• إجمالي اللاعبين المسجلين: {total_registered_players}\n"
        results_text += f"• عدد من شارك بإجابات: {total_participants_who_answered}\n"
        results_text += f"• عدد لم يشارك بإجابات: {not_answered_count}\n\n"
        results_text += f"{BLOCKQUOTE_CLOSE_TAG}"


        if len(sorted_participants) > 0:
            results_text += f"🏅 <b>لوحة المتصدرين:</b>\n"
            leaderboard_content = ""
            for i, (user_id, data) in enumerate(sorted_participants[:30]): # عرض أعلى 30 متسابقاً
                rank_emoji = ""
                if i == 0: rank_emoji = "🥇 "
                elif i == 1: rank_emoji = "🥈 "
                elif i == 2: rank_emoji = "🥉 "

                leaderboard_content += f"{rank_emoji}{ltr}{i+1}. {data['username']}: {data['score']}{pdf} نقطة\n"

            # **تطبيق تنسيق Blockquote expandable لطي لوحة المتصدرين**
            results_text += f"{BLOCKQUOTE_OPEN_TAG}\n{leaderboard_content}{BLOCKQUOTE_CLOSE_TAG}\n"
        else:
            results_text += "😔 لا توجد نتائج لعرضها.\n"

        # إضافة التوقيع إلى الرسالة النهائية
        if signature_text:
            results_text += f"\n{signature_text}"


        try:
            logger.info(f"Worker: [{quiz_key}] Saving quiz history and updating user stats in SQLite DB: {stats_db_path}")
            quiz_history_id = await sqlite_handler.save_quiz_history(stats_db_path, quiz_identifier, total_questions, winner_id, winner_score)

            for user_id, data in final_scores.items():
                total_points = data['score']
                username_for_db = data['username']
                correct_answers_count = sum(1 for q_score in data['answers'].values() if q_score > 0)
                total_answered_questions_count = len(data['answers']) # عدد الأسئلة التي أجاب عليها هذا المستخدم
                wrong_answers_count = total_answered_questions_count - correct_answers_count

                await sqlite_handler.update_user_stats(stats_db_path, user_id, username_for_db, total_points, correct_answers_count, wrong_answers_count)
                await sqlite_handler.save_quiz_participant(stats_db_path, quiz_history_id, user_id, total_points, data['answers'])

            logger.info(f"Worker: [{quiz_key}] Quiz results saved to SQLite successfully.")
        except Exception as e:
            logger.error(f"Worker: [{quiz_key}] Failed to save quiz results to SQLite: {e}", exc_info=True)

        message_data = {"text": results_text, "reply_markup": json.dumps({}), "parse_mode": "HTML"}
        await _send_telegram_update(quiz_key, telegram_bot, message_data, quiz_status)
        logger.info(f"Worker: [{quiz_key}] Final results message sent to Telegram.")

    finally:
        await redis_handler.end_quiz(bot_token, quiz_identifier)
        await redis_handler.redis_client.delete(lock_key)
        logger.info(f"Worker: [{quiz_key}] Quiz has been cleaned up from Redis and lock released.")


async def main_loop():
    logger.info("Worker: Starting main loop...")
    ignore_keywords = [
        ":askquestion",
        ":newpost",
        ":Newpost",
        ":stats",
        ":leaderboard",
        ":start",
        "Panel"
    ]

    while True:
        try:
            all_quiz_keys = [key async for key in redis_handler.redis_client.scan_iter("Quiz:*:*")]

            active_quiz_keys_to_process = [
                key for key in all_quiz_keys
                if not any(keyword in key for keyword in ignore_keywords)
            ]

            if active_quiz_keys_to_process:
                logger.debug(f"Worker: Found {len(active_quiz_keys_to_process)} quiz keys to process.")
                tasks = [process_active_quiz(key) for key in active_quiz_keys_to_process]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Worker: An error occurred while processing quiz {active_quiz_keys_to_process[i]}: {result}", exc_info=result)
            else:
                logger.debug("Worker: No active quizzes (after filtering) found. Waiting...")

        except Exception as e:
            logger.error(f"Worker: An critical error occurred in the main loop: {e}", exc_info=True)

        await asyncio.sleep(1.5)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Worker: Shutting down gracefully.")
    except Exception as e:
        logger.error(f"Worker: Fatal error in worker startup/main: {e}", exc_info=True)