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

# **إعدادات البوت**
ADMIN_TELEGRAM_ID = 6198033039

# **إعدادات منظم الطلبات**
bot_rate_limiter = {}
RATE_LIMIT_TOKENS_PER_SECOND = 2.0
RATE_LIMIT_BUCKET_SIZE = 10

# تعريف علامات HTML لطي النص
BLOCKQUOTE_OPEN_TAG = "<blockquote expandable>"
BLOCKQUOTE_CLOSE_TAG = "</blockquote>"

# **إعدادات جديدة للمسابقات**
ROUND_RESULT_DISPLAY_DURATION_SECONDS = 4 # مدة عرض نتائج الجولة (4 ثواني)

def get_telegram_bot(token: str) -> TelegramBotServiceAsync:
    if token not in bot_instances:
        bot_instances[token] = TelegramBotServiceAsync(token)
    return bot_instances[token]

async def send_admin_notification(bot_token: str, message: str):
    if not ADMIN_TELEGRAM_ID:
        logger.warning("Admin Telegram ID is not configured. Cannot send admin notification.")
        return
    admin_bot = get_telegram_bot(bot_token)
    try:
        if len(message) > 4000:
            message = message[:4000] + "\n... (الرسالة مختصرة)"
        message_data = {
            "chat_id": ADMIN_TELEGRAM_ID,
            "text": f"🚨 <b>إشعار خطأ من البوت</b> 🚨\n\n{message}",
            "parse_mode": "HTML"
        }
        await admin_bot.send_message(message_data)
        logger.info(f"Admin notification sent to {ADMIN_TELEGRAM_ID}.")
    except Exception as e:
        logger.error(f"Failed to send admin notification to {ADMIN_TELEGRAM_ID}: {e}", exc_info=True)

async def _is_api_call_allowed(bot_token: str, wait_for_tokens: bool = False) -> bool:
    now = datetime.now()
    if bot_token not in bot_rate_limiter:
        bot_rate_limiter[bot_token] = {
            "last_call_time": now, "tokens": RATE_LIMIT_BUCKET_SIZE, "last_retry_after": now
        }
    bucket = bot_rate_limiter[bot_token]
    if (now - bucket["last_retry_after"]).total_seconds() < 0:
        logger.debug(f"Worker: Rate limit for bot {bot_token}. Still in retry_after period.")
        return False
    time_passed = (now - bucket["last_call_time"]).total_seconds()
    bucket["tokens"] = min(RATE_LIMIT_BUCKET_SIZE, bucket["tokens"] + time_passed * RATE_LIMIT_TOKENS_PER_SECOND)
    bucket["last_call_time"] = now
    if bucket["tokens"] >= 1.0:
        bucket["tokens"] -= 1.0
        return True
    elif wait_for_tokens:
        wait_time = (1.0 - bucket["tokens"]) / RATE_LIMIT_TOKENS_PER_SECOND
        logger.debug(f"Worker: Rate limit for bot {bot_token}. Waiting {wait_time:.2f}s for tokens.")
        await asyncio.sleep(wait_time)
        bucket["tokens"] = max(0.0, bucket["tokens"] - 1.0 + wait_time * RATE_LIMIT_TOKENS_PER_SECOND)
        return True
    else:
        return False

async def _send_telegram_update(quiz_key: str, telegram_bot: TelegramBotServiceAsync, message_data: dict, quiz_status: dict, is_new_message: bool = False):
    bot_token = quiz_status.get("bot_token")
    if not await _is_api_call_allowed(bot_token, wait_for_tokens=True):
        logger.debug(f"Worker: [{quiz_key}] Telegram update skipped/delayed due to rate limit for bot {bot_token}.")
        return None
    inline_message_id = quiz_status.get("inline_message_id")
    chat_id = quiz_status.get("chat_id") # قد يكون هذا فارغاً إذا كان من Redis وتم تخزينه فارغاً
    message_id = quiz_status.get("message_id")
    now = datetime.now()

    # التأكد من أن chat_id هو عدد صحيح أو None
    try:
        chat_id_int = int(chat_id) if chat_id else None
    except ValueError:
        chat_id_int = None
        logger.warning(f"Worker: [{quiz_key}] معرف المحادثة غير صالح '{chat_id}' من Redis. سيتم تعيينه إلى None.")

    try:
        response = None
        if is_new_message: # لإرسال رسائل جديدة (مثل نتائج الجولة، إنهاء المسابقة إذا تم حذف الأصلية)
            if chat_id_int:
                message_data["chat_id"] = chat_id_int
                response = await asyncio.wait_for(telegram_bot.send_message(message_data), timeout=10.0)
            else:
                error_msg = f"لا يمكن إرسال رسالة جديدة للمسابقة {quiz_key}، معرف المحادثة مفقود/غير صالح: {chat_id_int}."
                logger.error(f"Worker: [{quiz_key}] {error_msg}")
                return None
        elif inline_message_id:
            message_data["inline_message_id"] = inline_message_id
            response = await asyncio.wait_for(telegram_bot.edit_inline_message(message_data), timeout=10.0)
        elif chat_id_int and message_id:
            message_data["chat_id"] = chat_id_int
            message_data["message_id"] = message_id
            response = await asyncio.wait_for(telegram_bot.edit_message(message_data), timeout=10.0)
        else:
            error_msg = f"لا يوجد معرف رسالة صالح (inline_message_id أو chat_id/message_id) للتعديل."
            logger.error(f"Worker: [{quiz_key}] {error_msg} لا يمكن إرسال التحديث.")
            await redis_handler.redis_client.hset(quiz_key, "status", "stopping")
            await send_admin_notification(bot_token, f"<b>خطأ حرج في المسابقة:</b> {quiz_key}\nالسبب: {error_msg}\nالمسابقة تم إيقافها.")
            return None

        if not response.get("ok"):
            desc = response.get("description", "")
            critical_error, error_reason = False, ""

            if "message to edit not found" in desc or "MESSAGE_ID_INVALID" in desc:
                critical_error, error_reason = True, "الرسالة الأصلية للمسابقة تم حذفها أو لم تعد صالحة."
                logger.warning(f"Worker: [{quiz_key}] خطأ حرج: رسالة المسابقة ربما حُذفت. السبب: {desc}")
                await redis_handler.redis_client.hset(quiz_key, mapping={
                    "status": "stopping",
                    "stop_reason": "message_deleted"
                })
                await send_admin_notification(
                    bot_token,
                    f"<b>خطأ حرج في المسابقة (رسالة محذوفة):</b> {quiz_key}\n"
                    f"معرف الرسالة: <code>{message_id}</code>\n"
                    f"معرف المحادثة: <code>{chat_id}</code>\n"
                    f"السبب: {error_reason}\n"
                    f"وصف تيليجرام: {html.escape(desc)}\n"
                    f"المسابقة تم إيقافها وسيتم إعلام المنشئ."
                )
                return response

            elif "bot was blocked by the user" in desc: critical_error, error_reason = True, "البوت تم حظره."
            elif "chat not found" in desc: critical_error, error_reason = True, "المحادثة غير موجودة."
            elif "message is not modified" in desc:
                logger.debug(f"Worker: [{quiz_key}] تيليجرام أفاد: {desc}")
                return response
            elif "Too Many Requests" in desc:
                retry_after = response.get("parameters", {}).get("retry_after", 5)
                logger.warning(f"Worker: [{quiz_key}] طلبات كثيرة جداً. إعادة المحاولة بعد {retry_after} ثانية.")
                bot_rate_limiter[bot_token]["last_retry_after"] = now + timedelta(seconds=retry_after)
                return None
            else:
                logger.error(f"Worker: [{quiz_key}] تيليجرام أفاد بفشل تحديث العرض: {desc}")
                return response

            if critical_error:
                logger.warning(f"Worker: [{quiz_key}] خطأ تيليجرام حرج ({error_reason}). تعيين المسابقة إلى 'stopping'.")
                await redis_handler.redis_client.hset(quiz_key, "status", "stopping")
                await send_admin_notification(bot_token, f"<b>خطأ حرج في المسابقة:</b> {quiz_key}\nمعرف الرسالة: <code>{message_id}</code>\nمعرف المحادثة: <code>{chat_id}</code>\nالسبب: {error_reason}\nوصف تيليجرام: {html.escape(desc)}\nالمسابقة تم إيقافها.")
        else:
            logger.debug(f"Worker: [{quiz_key}] تم تحديث رسالة العرض بنجاح.")
            if bot_token in bot_rate_limiter: bot_rate_limiter[bot_token]["last_retry_after"] = now

        return response

    except asyncio.TimeoutError:
        logger.warning(f"Worker: [{quiz_key}] انتهت المهلة أثناء محاولة إرسال تحديث تيليجرام.")
    except Exception as e:
        logger.error(f"Worker: [{quiz_key}] فشل إرسال تحديث تيليجرام بسبب استثناء: {e}", exc_info=True)
        await send_admin_notification(bot_token, f"<b>خطأ غير متوقع أثناء تحديث الرسالة للمسابقة:</b> {quiz_key}\nالسبب: {html.escape(str(e))}\nالرجاء التحقق من سجلات الخادم.")
    return None

async def update_pending_display(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync, force_update: bool = False):
    UPDATE_INTERVAL_SECONDS = 2.0
    now = datetime.now()
    if not force_update:
        last_update_str = await redis_handler.redis_client.hget(quiz_key, "last_display_update")
        if last_update_str:
            try:
                if (now - datetime.fromisoformat(last_update_str)).total_seconds() < UPDATE_INTERVAL_SECONDS: return
            except (ValueError, TypeError): logger.warning(f"Worker: [{quiz_key}] لم يتمكن من تحليل طابع الوقت last_display_update: {last_update_str}")
    bot_token = quiz_status.get("bot_token")
    if not await _is_api_call_allowed(bot_token, wait_for_tokens=True):
        logger.debug(f"Worker: [{quiz_key}] تحديث العرض المعلق تأخر بسبب حد المعدل للبوت {bot_token}.")
        return
    await redis_handler.redis_client.hset(quiz_key, "last_display_update", now.isoformat())
    logger.info(f"Worker: [{quiz_key}] المضي قدماً في تحديث العرض المعلق (force_update={force_update}).")
    players_json = quiz_status.get('players', '[]')
    try: players = json.loads(players_json)
    except json.JSONDecodeError: logger.warning(f"Worker: [{quiz_key}] لم يتمكن من فك ترميز JSON 'players': {players_json}. يفترض عدم وجود لاعبين."); players = []
    players_count = len(players)
    creator_name = html.escape(quiz_status.get('creator_username', 'غير معروف'))
    quiz_type = html.escape(quiz_status.get('quiz_type', 'عامة'))
    quiz_game_id = quiz_status.get('quiz_identifier', 'N/A')
    creator_user_id = quiz_status.get('creator_id')
    max_players = int(quiz_status.get("max_players", 12))
    players_list_str = "\n".join([f"{i+1}- {html.escape(p.get('username', 'مجهول'))}" for i, p in enumerate(players[:10])])
    if not players_list_str: players_list_str = "لا يوجد لاعبون بعد."
    message_text = (f"🎮 <b>مسابقة أسئلة جديدة!</b>\n\n"
                    f"🎯 <b>الفئة</b>: {quiz_type}\n"
                    f"👤 <b>المنشئ</b>: {creator_name}\n\n"
                    f"👥 <b>اللاعبون ({players_count}/{max_players}):</b>\n{players_list_str}")
    buttons = {"inline_keyboard": [[{"text": '➡️ انضم للمسابقة', "callback_data": f"quiz_join|{quiz_game_id}|{creator_user_id}"}], [{"text": '▶️ ابدأ المسابقة', "callback_data": f"quiz_start|{quiz_game_id}|{creator_user_id}"}]]}
    if players_count >= max_players:
        for row_idx, row in enumerate(buttons["inline_keyboard"]):
            for btn_idx, button in enumerate(row):
                if button.get("callback_data") and button["callback_data"].startswith("quiz_join"):
                    buttons["inline_keyboard"][row_idx][btn_idx] = {"text": '👥 العدد مكتمل', "callback_data": 'ignore_full_quiz'}; break
            else: continue
            break
    message_data = {"text": message_text, "reply_markup": json.dumps(buttons), "parse_mode": "HTML"}
    await _send_telegram_update(quiz_key, telegram_bot, message_data, quiz_status)

async def update_question_display(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync, time_left: float):
    """
    دالة بسيطة لتحديث نص الرسالة وعرض الوقت المتبقي.
    """
    logger.debug(f"Worker: [{quiz_key}] تهيئة رسالة تحديث منتصف المسابقة.")

    category_display_name = quiz_status.get("category_display_name", "عامة")
    base_question_text_from_redis = quiz_status.get("current_question_text", "")
    participants = int(quiz_status.get("participant_count", 0)) # عدد المشاركين النشطين

    if not base_question_text_from_redis:
        return

    new_text = (
        f"❓ {base_question_text_from_redis}\n\n"
        f"🏷️ <b>الفئة</b>: {html.escape(category_display_name)}\n"
        f"👥 <b>المشاركون</b>: {participants}\n"
        f"⏳ <b>الوقت المتبقي</b>: {int(time_left + 0.99)} ثانية")

    current_keyboard_str = quiz_status.get("current_keyboard")
    if not current_keyboard_str:
        return

    message_data = {
        "text": new_text,
        "reply_markup": current_keyboard_str, # الكيبورد موجود بالفعل كـ JSON string
        "parse_mode": "HTML"
    }

    await _send_telegram_update(quiz_key, telegram_bot, message_data, quiz_status)

async def display_round_results(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    """
    يعرض نتائج الجولة الحالية (السؤال الذي انتهى وقته للتو).
    """
    logger.info(f"Worker: [{quiz_key}] عرض نتائج الجولة.")

    quiz_identifier = quiz_status.get("quiz_identifier")
    current_index = int(quiz_status.get("current_index", -1)) # هذا يمثل السؤال الذي انتهت للتو جولته
    question_ids = json.loads(quiz_status.get("question_ids", "[]"))

    if current_index < 0 or current_index >= len(question_ids):
        logger.warning(f"Worker: [{quiz_key}] معرف السؤال الحالي غير صالح {current_index} لعرض نتائج الجولة. تخطي.")
        return

    question_id_for_results = question_ids[current_index]
    questions_db_path = quiz_status.get("questions_db_path")
    question = await sqlite_handler.get_question_by_id(questions_db_path, question_id_for_results)

    if not question:
        logger.error(f"Worker: [{quiz_key}] السؤال {question_id_for_results} غير موجود لنتائج الجولة. تخطي.")
        return

    correct_option_index = question['correct_opt']
    correct_answer_text = question[f"opt{correct_option_index + 1}"]

    players_answers_for_this_question = await redis_handler.get_player_answers_for_question(
        quiz_status["bot_token"], quiz_identifier, question_id_for_results
    )

    correct_answers_list = []
    wrong_answers_list = []
    eliminated_players_list = []

    # التحقق من إعداد الإقصاء
    eliminate_after = int(quiz_status.get("eliminate_after", -1)) # -1 يعني معطل

    for user_id, answer_data in players_answers_for_this_question.items():
        username = html.escape(answer_data.get('username', f"لاعب {user_id}"))
        score = answer_data.get('score', 0.0)
        time_taken = answer_data.get('time', 0.0)
        is_correct = answer_data.get('correct', False)
        wrong_answers_total = answer_data.get('wrong_answers_total', 0) # إجمالي الإجابات الخاطئة حتى الآن

        if is_correct:
            correct_answers_list.append(f"✅ {username} (+{score:.2f} نقطة، {time_taken:.2f} ثانية)")
        else:
            wrong_answers_list.append(f"❌ {username}")
            # التحقق من الإقصاء
            if eliminate_after > 0 and wrong_answers_total >= eliminate_after:
                # التأكد من عدم إقصاء اللاعبين الذين أجابوا بشكل صحيح في هذه الجولة
                # هذه قائمة بأسماء اللاعبين الذين تم إقصائهم في هذه الجولة أو قبلها وظهروا الآن
                if answer_data.get('eliminated') != "1": # إذا لم يتم إقصاؤه بالفعل
                    eliminated_players_list.append(username)
                    await redis_handler.redis_client.hset(redis_handler.quiz_answers_key(quiz_status["bot_token"], quiz_identifier, user_id), "eliminated", "1")
                    logger.info(f"Worker: [{quiz_key}] تم إقصاء اللاعب {username} ({user_id}) بعد {wrong_answers_total} إجابات خاطئة.")

    round_results_text = f"🏁 <b>نهاية السؤال رقم {current_index + 1}</b> 🏁\n\n"
    round_results_text += f"💡 <b>الإجابة الصحيحة:</b> {html.escape(correct_answer_text)}\n\n"

    if correct_answers_list:
        round_results_text += "✅ <b>الإجابات الصحيحة:</b>\n" + "\n".join(correct_answers_list) + "\n\n"
    else:
        round_results_text += "😔 لا توجد إجابات صحيحة في هذه الجولة.\n\n"

    if wrong_answers_list:
        round_results_text += "❌ <b>الإجابات الخاطئة:</b>\n" + "\n".join(wrong_answers_list) + "\n\n"

    if eliminated_players_list:
        round_results_text += f"🚫 <b>تم إقصاء اللاعبين:</b>\n" + "\n".join(eliminated_players_list) + "\n\n"

    message_data = {
        "text": round_results_text,
        "reply_markup": json.dumps({}), # إزالة الأزرار لنتائج الجولة
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    # إرسال التحديث (هذا سيكون تعديل لرسالة المسابقة الرئيسية)
    await _send_telegram_update(quiz_key, telegram_bot, message_data, quiz_status)

    # تخزين طابع الوقت لهذا العرض لفرض التوقف
    await redis_handler.redis_client.hset(quiz_key, "round_results_displayed_at", datetime.now().isoformat())


async def process_active_quiz(quiz_key: str):
    processing_lock_key = f"Lock:Process:{quiz_key}"
    if not await redis_handler.redis_client.set(processing_lock_key, "true", ex=10, nx=True):
        return

    try:
        quiz_status = await redis_handler.get_quiz_status_by_key(quiz_key)
        if not quiz_status: return

        bot_token = quiz_status.get("bot_token")
        quiz_identifier = quiz_status.get("quiz_identifier")

        if not bot_token or not quiz_identifier:
            await redis_handler.redis_client.delete(quiz_key)
            return

        telegram_bot = get_telegram_bot(bot_token)
        status = quiz_status.get("status")

        if status == "stopping":
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        if status == "pending":
            await update_pending_display(quiz_key, quiz_status, telegram_bot)
            return

        if status != "active": return

        quiz_time_key_str = redis_handler.quiz_time_key(bot_token, quiz_identifier)
        quiz_time = await redis_handler.redis_client.hgetall(quiz_time_key_str)

        current_question_ended = False
        if quiz_time and "end" in quiz_time:
            try:
                end_time = datetime.fromisoformat(quiz_time["end"])
                if datetime.now() >= end_time:
                    current_question_ended = True
                else:
                    time_left = (end_time - datetime.now()).total_seconds()
                    time_per_question = int(quiz_status.get('time_per_question', 30))
                    mid_update_sent = quiz_status.get('mid_update_sent', '0') == '1'

                    if time_left <= (time_per_question / 2) and not mid_update_sent:
                        logger.info(f"Worker: [{quiz_key}] وصلت لنقطة المنتصف. إرسال تحديث منتصف المسابقة.")
                        await update_question_display(quiz_key, quiz_status, telegram_bot, time_left)
                        await redis_handler.redis_client.hset(quiz_key, "mid_update_sent", "1")

            except (ValueError, TypeError):
                current_question_ended = True # التعامل مع خطأ التحليل على أنه انتهاء الوقت
        else:
            current_question_ended = True # إذا لم يكن هناك مؤقت، انتقل إلى السؤال التالي فوراً

        if current_question_ended:
            round_results_displayed_at_str = quiz_status.get("round_results_displayed_at")
            if round_results_displayed_at_str:
                try:
                    round_display_time = datetime.fromisoformat(round_results_displayed_at_str)
                    if (datetime.now() - round_display_time).total_seconds() < ROUND_RESULT_DISPLAY_DURATION_SECONDS:
                        # لا يزال يتم عرض نتائج الجولة، انتظر
                        logger.debug(f"Worker: [{quiz_key}] في انتظار انتهاء مدة عرض نتائج الجولة.")
                        return # لا تعالج السؤال التالي بعد
                    else:
                        # انتهت مدة عرض نتائج الجولة، امسح العلامة وتابع إلى السؤال التالي
                        await redis_handler.redis_client.hdel(quiz_key, "round_results_displayed_at")
                        await handle_next_question(quiz_key, quiz_status, telegram_bot)
                except (ValueError, TypeError):
                    logger.warning(f"Worker: [{quiz_key}] لم يتمكن من تحليل طابع الوقت round_results_displayed_at: {round_results_displayed_at_str}. المضي قدماً.")
                    await handle_next_question(quiz_key, quiz_status, telegram_bot)
            else:
                # انتهى السؤال، لكن نتائج الجولة لم تُعرض بعد. اعرضها الآن.
                logger.info(f"Worker: [{quiz_key}] انتهى وقت السؤال. عرض نتائج الجولة قبل السؤال التالي.")
                await display_round_results(quiz_key, quiz_status, telegram_bot)
                return # انتظر التكرار التالي في الحلقة لمعالجة مدة العرض

        # إذا لم يكن السؤال الحالي قد انتهى بعد، فسيتم التعامل مع ذلك بواسطة update_question_display في الأجزاء السابقة.
        # لذا، لا يوجد المزيد من `should_process_next_question` هنا إلا بعد انتهاء مدة عرض النتائج.

    finally:
        await redis_handler.redis_client.delete(processing_lock_key)


async def handle_next_question(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    current_index = int(quiz_status.get("current_index", -1))

    # جلب عدد المشاركين الحاليين (باستثناء اللاعبين المقُصّين)
    quiz_identifier = quiz_status.get("quiz_identifier")
    active_players_count = 0
    async for key in redis_handler.redis_client.scan_iter(f"QuizAnswers:{quiz_status['bot_token']}:{quiz_identifier}:*"):
        user_data = await redis_handler.redis_client.hgetall(key)
        if user_data.get("eliminated") != "1":
            active_players_count += 1

    # إذا تم إقصاء جميع اللاعبين النشطين، قم بإنهاء المسابقة
    if active_players_count == 0:
        logger.info(f"Worker: [{quiz_key}] تم إقصاء جميع اللاعبين النشطين. إنهاء المسابقة.")
        await redis_handler.redis_client.hset(quiz_key, "status", "stopping")
        return # سيتعامل معها `end_quiz` في الحلقة التالية

    try:
        question_ids = json.loads(quiz_status.get("question_ids", "[]"))
    except json.JSONDecodeError:
        await end_quiz(quiz_key, quiz_status, telegram_bot)
        return

    next_index = current_index + 1
    logger.info(f"Worker: [{quiz_key}] معالجة منطق السؤال التالي. المؤشر التالي: {next_index}")

    if next_index < len(question_ids):
        next_question_id = question_ids[next_index]
        questions_db_path = quiz_status.get("questions_db_path")
        if not questions_db_path:
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        question = await sqlite_handler.get_question_by_id(questions_db_path, next_question_id)
        if not question:
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        base_question_text_for_redis = f"<b>السؤال  {next_index + 1} </b>:\n{question['question']}"
        options = [question['opt1'], question['opt2'], question['opt3'], question['opt4']]
        quiz_identifier_for_callbacks = quiz_status.get("quiz_identifier")
        keyboard = {"inline_keyboard": [[{"text": opt, "callback_data": f"answer_{quiz_identifier_for_callbacks}_{next_question_id}_{i}"}] for i, opt in enumerate(options)]}
        time_per_question = int(quiz_status.get("time_per_question", 30))
        category_display_name = quiz_status.get("category_display_name", "عامة")

        full_new_question_message_text = (
            f"❓ {base_question_text_for_redis}\n\n"
            f"🏷️ <b>الفئة</b>: {html.escape(category_display_name)}\n"
            f"👥 <b>المشاركون</b>: {active_players_count}\n" # استخدام عدد اللاعبين النشطين
            f"⏳ <b>الوقت المتبقي</b>: {time_per_question} ثانية"
        )

        end_time = datetime.now() + timedelta(seconds=time_per_question)
        bot_token = quiz_status.get("bot_token")

        await redis_handler.redis_client.hset(
            quiz_key, mapping={
                "current_question_text": base_question_text_for_redis,
                "current_keyboard": json.dumps(keyboard),
                "current_index": next_index,
                "mid_update_sent": "0",  # إعادة الضبط للسؤال التالي
                "participant_count": active_players_count # تحديث عدد المشاركين في hash المسابقة الرئيسي
            }
        )
        await redis_handler.set_current_question(bot_token, quiz_identifier, next_question_id, end_time)

        message_data = {
            "text": full_new_question_message_text,
            "reply_markup": json.dumps(keyboard),
            "parse_mode": "HTML"
        }
        await _send_telegram_update(quiz_key, telegram_bot, message_data, quiz_status)

    else:
        logger.info(f"Worker: [{quiz_key}] تمت معالجة جميع الأسئلة. إنهاء المسابقة.")
        await end_quiz(quiz_key, quiz_status, telegram_bot)


async def end_quiz(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    lock_key = f"Lock:EndQuiz:{quiz_key}"
    if not await redis_handler.redis_client.set(lock_key, "true", ex=60, nx=True):
        logger.warning(f"Worker: [{quiz_key}] عملية الإنهاء مقفلة بالفعل. تخطي.")
        return
    try:
        logger.info(f"Worker: [{quiz_key}] بدء عملية end_quiz (تم الحصول على القفل).")
        bot_token = quiz_status.get("bot_token")
        quiz_identifier = quiz_status.get("quiz_identifier")
        stats_db_path = quiz_status.get("stats_db_path")
        signature_text = quiz_status.get("signature_text", "بـوت تـحدي الاسئلة ❓ (https://t.me/nniirrbot)")

        # الحصول على chat_id لتخزين السجل
        chat_id_for_history = quiz_status.get("chat_id")
        try:
            chat_id_for_history = int(chat_id_for_history) if chat_id_for_history else None
        except ValueError:
            chat_id_for_history = None
            logger.warning(f"Worker: [{quiz_key}] معرف المحادثة غير صالح '{quiz_status.get('chat_id')}' في quiz_status للسجل. سيتم تعيينه إلى None.")

        if not bot_token or not quiz_identifier:
            logger.error(f"Worker: [{quiz_key}] لا يمكن إنهاء المسابقة، مفقود bot_token أو quiz_identifier.")
            return

        total_questions = len(json.loads(quiz_status.get("question_ids", "[]")))
        logger.info(f"Worker: [{quiz_key}] حساب النتائج من Redis.")

        final_scores = {}
        total_participants_who_answered = 0
        eliminated_count = 0

        async for key in redis_handler.redis_client.scan_iter(f"QuizAnswers:{bot_token}:{quiz_identifier}:*"):
            try:
                user_id = int(key.split(":")[-1])
                user_data = await redis_handler.redis_client.hgetall(key)

                # استخدام "total_score" وهو من نوع float
                score = float(user_data.get('total_score', 0.0))
                username = html.escape(user_data.get('username', f"مستخدم_{user_id}"))

                # جمع جميع إجابات الأسئلة الفردية لحقل `answers` في quiz_participants
                user_answers_detailed = {}
                for hkey, hval in user_data.items():
                    if hkey.startswith('q_data:'):
                        try:
                            # مفتاح q_data:QUESTION_ID يخزن JSON {correct, score, time, question_id}
                            q_data_parsed = json.loads(hval)
                            user_answers_detailed[q_data_parsed.get('question_id')] = q_data_parsed
                        except (ValueError, json.JSONDecodeError):
                            logger.warning(f"Worker: [{quiz_key}] لم يتمكن من تحليل q_data للمستخدم {user_id}, مفتاح {hkey}.")

                final_scores[user_id] = {
                    'total_score': score,
                    'username': username,
                    'answers': user_answers_detailed, # تفاصيل الإجابات لكل سؤال
                    'correct_answers_count': int(user_data.get('correct_answers_count', 0)),
                    'wrong_answers_count': int(user_data.get('wrong_answers_count', 0)),
                    'eliminated': user_data.get('eliminated') == '1' # التحقق مما إذا تم إقصاء اللاعب
                }
                if user_data.get('eliminated') == '1':
                    eliminated_count += 1

                # حساب المشاركين الذين أجابوا على الأقل سؤالاً واحداً بشكل صحيح أو خاطئ
                if len(user_answers_detailed) > 0: total_participants_who_answered += 1

            except (ValueError, IndexError, TypeError) as e:
                logger.warning(f"Worker: [{quiz_key}] لم يتمكن من تحليل بيانات المستخدم من مفتاح الإجابة '{key}': {e}")

        # إجمالي اللاعبين المسجلين من إعداد المسابقة الأولي
        registered_players_json = quiz_status.get('players', '[]')
        try: total_registered_players = len(json.loads(registered_players_json))
        except json.JSONDecodeError: logger.warning(f"Worker: [{quiz_key}] لم يتمكن من فك ترميز JSON 'players'."); total_registered_players = 0

        # حساب اللاعبين الذين انضموا ولكن لم يجيبوا
        not_answered_count = max(0, total_registered_players - total_participants_who_answered)

        # الفرز حسب total_score (float)
        sorted_participants = sorted(final_scores.items(), key=lambda item: item[1]['total_score'], reverse=True)

        winner_id, winner_score, winner_username_escaped = (None, 0.0, "لا يوجد")
        if sorted_participants:
            winner_id, winner_data = sorted_participants[0]
            winner_score = winner_data['total_score']
            try:
                # محاولة الحصول على معلومات مستخدم تيليجرام الكاملة للفائز
                # استخدام creator_id أو chat_id_for_history لـ chat_id في get_chat_member
                # هذا سيفشل إذا كان الفائز ليس في نفس المحادثة أو لم يكن البوت عضواً فيها.
                # الأسلوب الأكثر أماناً هو استخدام user_id نفسه لـ chat_id إذا كان المستخدم يسمح بالبوت في PM.
                get_user_info = await telegram_bot.get_chat_member(chat_id=int(quiz_status.get('creator_id', winner_id)), user_id=int(winner_id))
                if get_user_info and get_user_info.get("ok"):
                    user_api_data = get_user_info.get("result", {})
                    if user_api_data.get("username"): winner_username_escaped = f"@{html.escape(user_api_data['username'])}"
                    elif user_api_data.get("first_name"): winner_username_escaped = f"<a href='tg://user?id={user_api_data['id']}'>{html.escape(user_api_data['first_name'])}</a>"
                    else: winner_username_escaped = winner_data.get('username', f"مستخدم_{winner_id}")
                else: winner_username_escaped = winner_data.get('username', f"مستخدم_{winner_id}")
            except Exception as e:
                logger.warning(f"Worker: [{quiz_key}] لم يتمكن من جلب معلومات الفائز: {e}"); winner_username_escaped = winner_data.get('username', f"مستخدم_{winner_id}")

        ltr, pdf = '\u202A', '\u202C' # لنص ثنائي الاتجاه في HTML

        results_text = "🏆 <b>المسابقة انتهت! النتائج النهائية:</b> 🏆\n\n"
        if winner_id:
            results_text += f"🎉 <b>الفائز</b>: {ltr}{winner_username_escaped}{pdf} بـ {winner_score:.2f} نقطة!\n\n"
        else:
            results_text += "😞 لم يشارك أحد في المسابقة أو لم يحصل أحد على نقاط.\n\n"

        results_text += f"{BLOCKQUOTE_OPEN_TAG}"
        results_text += f"📊 <b>إحصائيات المشاركة:</b>\n"
        results_text += f"• إجمالي اللاعبين المسجلين: {total_registered_players}\n"
        results_text += f"• عدد من شارك بإجابات: {total_participants_who_answered}\n"
        results_text += f"• عدد لم يشارك بإجابات: {not_answered_count}\n"
        # عرض عدد اللاعبين الذين تم إقصاؤهم إذا كان نظام الإقصاء مفعلاً
        if int(quiz_status.get("eliminate_after", -1)) > 0:
            results_text += f"• عدد اللاعبين الذين تم إقصاؤهم: {eliminated_count}\n"
        results_text += f"{BLOCKQUOTE_CLOSE_TAG}"

        if len(sorted_participants) > 0:
            results_text += f"\n🏅 <b>لوحة المتصدرين:</b>\n"
            leaderboard_content = ""
            for i, (user_id, data) in enumerate(sorted_participants[:30]):
                rank_emoji = ""
                if i == 0: rank_emoji = "🥇 "
                elif i == 1: rank_emoji = "🥈 "
                elif i == 2: rank_emoji = "🥉 "
                eliminated_tag = " 🚫 (مقصى)" if data['eliminated'] else ""
                leaderboard_content += f"{rank_emoji}{ltr}{i+1}. {data['username']}: {data['total_score']:.2f} نقطة{eliminated_tag}{pdf}\n"
            results_text += f"{BLOCKQUOTE_OPEN_TAG}\n{leaderboard_content}{BLOCKQUOTE_CLOSE_TAG}\n"
        else:
            results_text += "😔 لا توجد نتائج لعرضها.\n"

        if signature_text:
            results_text += f"\n{signature_text}"

        # حفظ سجل المسابقة وإحصائيات المستخدمين
        try:
            logger.info(f"Worker: [{quiz_key}] حفظ سجل المسابقة والإحصائيات إلى SQLite: {stats_db_path}")
            # تمرير chat_id_for_history إلى دالة save_quiz_history
            quiz_history_id = await sqlite_handler.save_quiz_history(stats_db_path, quiz_identifier, total_questions, winner_id, winner_score, chat_id_for_history)
            for user_id, data in final_scores.items():
                total_points = data['total_score']
                username_for_db = data['username']
                correct_answers_count = data['correct_answers_count']
                wrong_answers_count = data['wrong_answers_count']

                await sqlite_handler.update_user_stats(stats_db_path, user_id, username_for_db, total_points, correct_answers_count, wrong_answers_count)

                # 'answers' في final_scores هو user_answers_detailed (dict of question_id -> detailed answer data)
                await sqlite_handler.save_quiz_participant(stats_db_path, quiz_history_id, user_id, total_points, data['answers'])
            logger.info(f"Worker: [{quiz_key}] تم حفظ نتائج المسابقة في SQLite بنجاح.")
        except Exception as e:
            logger.error(f"Worker: [{quiz_key}] فشل حفظ نتائج المسابقة في SQLite: {e}", exc_info=True)
            await send_admin_notification(bot_token, f"<b>خطأ في حفظ نتائج المسابقة:</b> {quiz_key}\nالسبب: {html.escape(str(e))}\nالرجاء التحقق من SQLite.")

        stop_reason = quiz_status.get("stop_reason")

        if stop_reason == "message_deleted":
            logger.warning(f"Worker: [{quiz_key}] تم إيقاف المسابقة لأن الرسالة حُذفت. إرسال النتائج النهائية في رسالة جديدة للمنشئ.")
            creator_id = quiz_status.get("creator_id")
            if creator_id:
                notification_prefix = (
                    "⚠️ <b>تم إيقاف مسابقتك!</b> ⚠️\n\n"
                    "<b>السبب:</b> تم حذف رسالة المسابقة الأصلية من المجموعة، مما منع البوت من إكمالها.\n"
                    "هذه هي النتائج النهائية للمسابقة:\n"
                    "------------------------------------\n\n"
                )
                final_message_text = notification_prefix + results_text

                new_message_data = {
                    "chat_id": creator_id,
                    "text": final_message_text,
                    "parse_mode": "HTML",
                    'disable_web_page_preview': True
                }
                try:
                    # تمرير is_new_message=True لإرسال رسالة جديدة
                    await _send_telegram_update(quiz_key, telegram_bot, new_message_data, quiz_status, is_new_message=True)
                    logger.info(f"Worker: [{quiz_key}] تم إرسال النتائج النهائية بنجاح إلى المنشئ {creator_id}.")
                except Exception as e:
                    logger.error(f"Worker: [{quiz_key}] فشل إرسال النتائج النهائية إلى المنشئ {creator_id}: {e}", exc_info=True)
                    await send_admin_notification(bot_token, f"<b>فشل إرسال نتائج المسابقة للمنشئ:</b> {quiz_key}\nالمنشئ: {creator_id}\nالسبب: {html.escape(str(e))}")
            else:
                logger.error(f"Worker: [{quiz_key}] لا يمكن إعلام المنشئ بحذف الرسالة لأن creator_id مفقود.")
                await send_admin_notification(bot_token, f"<b>فشل إعلام منشئ المسابقة:</b> {quiz_key}\nالسبب: `creator_id` مفقود في بيانات المسابقة.")

        else:
            logger.info(f"Worker: [{quiz_key}] إرسال النتائج النهائية عن طريق تعديل الرسالة الأصلية.")
            message_data = {
                "text": results_text,
                "reply_markup": json.dumps({}), # إزالة الأزرار
                'disable_web_page_preview': True,
                "parse_mode": "HTML"
            }
            await _send_telegram_update(quiz_key, telegram_bot, message_data, quiz_status)

    finally:
        await redis_handler.end_quiz(bot_token, quiz_identifier)
        await redis_handler.redis_client.delete(lock_key)
        logger.info(f"Worker: [{quiz_key}] تم تنظيف المسابقة من Redis وتحرير القفل.")


async def main_loop():
    logger.info("Worker: بدء الحلقة الرئيسية...")
    ignore_keywords = [":askquestion", ":newpost", ":Newpost", ":stats", ":leaderboard", ":start", "Panel"]
    while True:
        try:
            all_quiz_keys = [key async for key in redis_handler.redis_client.scan_iter("Quiz:*:*")]
            active_quiz_keys_to_process = [key for key in all_quiz_keys if not any(keyword in key for keyword in ignore_keywords)]
            if active_quiz_keys_to_process:
                tasks = [process_active_quiz(key) for key in active_quiz_keys_to_process]
                await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Worker: حدث خطأ حرج في الحلقة الرئيسية: {e}", exc_info=True)

        await asyncio.sleep(0.2)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Worker: إيقاف التشغيل بأمان.")
    except Exception as e:
        logger.error(f"Worker: خطأ فادح في بدء تشغيل/البرنامج الرئيسي للعامل: {e}", exc_info=True)