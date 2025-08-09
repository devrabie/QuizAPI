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

def get_telegram_bot(token: str) -> TelegramBotServiceAsync:
    if token not in bot_instances:
        bot_instances[token] = TelegramBotServiceAsync(token)
    return bot_instances[token]

# ... (الكود من send_admin_notification إلى _send_telegram_update يبقى كما هو)
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

async def _send_telegram_update(quiz_key: str, telegram_bot: TelegramBotServiceAsync, message_data: dict, quiz_status: dict):
    bot_token = quiz_status.get("bot_token")
    if not await _is_api_call_allowed(bot_token, wait_for_tokens=True):
        logger.debug(f"Worker: [{quiz_key}] Telegram update skipped/delayed due to rate limit for bot {bot_token}.")
        return
    inline_message_id = quiz_status.get("inline_message_id")
    chat_id = quiz_status.get("chat_id")
    message_id = quiz_status.get("message_id")
    now = datetime.now()
    try:
        response = None
        if inline_message_id:
            message_data["inline_message_id"] = inline_message_id
            response = await asyncio.wait_for(telegram_bot.edit_inline_message(message_data), timeout=10.0)
        elif chat_id and message_id:
            message_data["chat_id"] = chat_id
            message_data["message_id"] = message_id
            response = await asyncio.wait_for(telegram_bot.edit_message(message_data), timeout=10.0)
        else:
            error_msg = f"No valid message identifier (inline_message_id OR chat_id/message_id) for editing."
            logger.error(f"Worker: [{quiz_key}] {error_msg} Cannot send update.")
            await redis_handler.redis_client.hset(quiz_key, "status", "stopping")
            await send_admin_notification(bot_token, f"<b>خطأ حرج في المسابقة:</b> {quiz_key}\nالسبب: {error_msg}\nالمسابقة تم إيقافها.")
            return
        if not response.get("ok"):
            desc = response.get("description", "")
            critical_error, error_reason = False, ""
            if "message to edit not found" in desc: critical_error, error_reason = True, "الرسالة التي يتم تعديلها غير موجودة."
            elif "MESSAGE_ID_INVALID" in desc: critical_error, error_reason = True, "معرف الرسالة غير صالح."
            elif "bot was blocked by the user" in desc: critical_error, error_reason = True, "البوت تم حظره."
            elif "chat not found" in desc: critical_error, error_reason = True, "المحادثة غير موجودة."
            elif "message is not modified" in desc: logger.debug(f"Worker: [{quiz_key}] Telegram reported: {desc}"); return
            elif "Too Many Requests" in desc:
                retry_after = response.get("parameters", {}).get("retry_after", 5)
                logger.warning(f"Worker: [{quiz_key}] Too Many Requests. Retrying after {retry_after}s.")
                bot_rate_limiter[bot_token]["last_retry_after"] = now + timedelta(seconds=retry_after)
                return
            else: logger.error(f"Worker: [{quiz_key}] Telegram reported failure to update display: {desc}")
            if critical_error:
                logger.warning(f"Worker: [{quiz_key}] Critical Telegram error ({error_reason}). Setting quiz to 'stopping'.")
                await redis_handler.redis_client.hset(quiz_key, "status", "stopping")
                await send_admin_notification(bot_token, f"<b>خطأ حرج في المسابقة:</b> {quiz_key}\nمعرف الرسالة: <code>{message_id}</code>\nمعرف المحادثة: <code>{chat_id}</code>\nالسبب: {error_reason}\nوصف تيليجرام: {html.escape(desc)}\nالمسابقة تم إيقافها.")
        else:
            logger.debug(f"Worker: [{quiz_key}] Successfully updated display message.")
            if bot_token in bot_rate_limiter: bot_rate_limiter[bot_token]["last_retry_after"] = now
    except asyncio.TimeoutError: logger.warning(f"Worker: [{quiz_key}] Timed out while trying to send Telegram update.")
    except Exception as e:
        logger.error(f"Worker: [{quiz_key}] Failed to send Telegram update due to an exception: {e}", exc_info=True)
        await send_admin_notification(bot_token, f"<b>خطأ غير متوقع أثناء تحديث الرسالة للمسابقة:</b> {quiz_key}\nالسبب: {html.escape(str(e))}\nالرجاء التحقق من سجلات الخادم.")

# ... (دالة update_pending_display تبقى كما هي)
async def update_pending_display(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync, force_update: bool = False):
    UPDATE_INTERVAL_SECONDS = 2.0
    now = datetime.now()
    if not force_update:
        last_update_str = await redis_handler.redis_client.hget(quiz_key, "last_display_update")
        if last_update_str:
            try:
                if (now - datetime.fromisoformat(last_update_str)).total_seconds() < UPDATE_INTERVAL_SECONDS: return
            except (ValueError, TypeError): logger.warning(f"Worker: [{quiz_key}] Could not parse last_display_update timestamp: {last_update_str}")
    bot_token = quiz_status.get("bot_token")
    if not await _is_api_call_allowed(bot_token, wait_for_tokens=True):
        logger.debug(f"Worker: [{quiz_key}] Pending display update delayed due to rate limit for bot {bot_token}.")
        return
    await redis_handler.redis_client.hset(quiz_key, "last_display_update", now.isoformat())
    logger.info(f"Worker: [{quiz_key}] Proceeding with pending display update (force_update={force_update}).")
    players_json = quiz_status.get('players', '[]')
    try: players = json.loads(players_json)
    except json.JSONDecodeError: logger.warning(f"Worker: [{quiz_key}] Could not decode 'players' JSON: {players_json}. Assuming no players."); players = []
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

# ## <-- التغيير هنا: أعدنا هذه الدالة ولكن بمنطق أبسط
async def update_question_display(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync, time_left: float):
    """
    دالة بسيطة لتحديث نص الرسالة وعرض الوقت المتبقي.
    """
    logger.debug(f"Worker: [{quiz_key}] Formatting mid-quiz update message.")

    category_display_name = quiz_status.get("category_display_name", "عامة")
    base_question_text_from_redis = quiz_status.get("current_question_text", "")
    participants = int(quiz_status.get("participant_count", 0))

    if not base_question_text_from_redis:
        return

    new_text = (
        f"❓ {base_question_text_from_redis}\n\n"
        f"🏷️ <b>الفئة</b>: {html.escape(category_display_name)}\n"
        f"👥 <b>المشاركون</b>: {participants}\n"
        f"⏳ <b>الوقت المتبقي</b>: {int(time_left + 0.99)} ثانية"
    )

    current_keyboard_str = quiz_status.get("current_keyboard")
    if not current_keyboard_str:
        return

    message_data = {
        "text": new_text,
        "reply_markup": current_keyboard_str, # الكيبورد موجود بالفعل كـ JSON string
        "parse_mode": "HTML"
    }

    await _send_telegram_update(quiz_key, telegram_bot, message_data, quiz_status)


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

        quiz_time_key = redis_handler.quiz_time_key(bot_token, quiz_identifier)
        quiz_time = await redis_handler.redis_client.hgetall(quiz_time_key)

        should_process_next_question = False
        if quiz_time and "end" in quiz_time:
            try:
                end_time = datetime.fromisoformat(quiz_time["end"])
                if datetime.now() >= end_time:
                    should_process_next_question = True
                else:
                    # ## <-- التغيير هنا: هذا هو المنطق الجديد لتحديث منتصف الوقت
                    time_left = (end_time - datetime.now()).total_seconds()

                    time_per_question = int(quiz_status.get('time_per_question', 30))
                    mid_update_sent = quiz_status.get('mid_update_sent', '0') == '1'

                    # التحقق إذا وصلنا لمنتصف الوقت ولم نرسل التحديث بعد
                    if time_left <= (time_per_question / 2) and not mid_update_sent:
                        logger.info(f"Worker: [{quiz_key}] Reached halfway point. Sending mid-quiz update.")

                        # استدعاء دالة التحديث
                        await update_question_display(quiz_key, quiz_status, telegram_bot, time_left)

                        # ضبط العلامة في Redis لمنع التحديث مرة أخرى
                        await redis_handler.redis_client.hset(quiz_key, "mid_update_sent", "1")

            except (ValueError, TypeError):
                should_process_next_question = True
        else:
            should_process_next_question = True

        if should_process_next_question:
            if await redis_handler.redis_client.hget(quiz_key, "status") == "stopping":
                return
            await handle_next_question(quiz_key, quiz_status, telegram_bot)

    finally:
        await redis_handler.redis_client.delete(processing_lock_key)


async def handle_next_question(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    current_index = int(quiz_status.get("current_index", -1))
    try:
        question_ids = json.loads(quiz_status.get("question_ids", "[]"))
    except json.JSONDecodeError:
        await end_quiz(quiz_key, quiz_status, telegram_bot)
        return

    next_index = current_index + 1
    logger.info(f"Worker: [{quiz_key}] Handling next question logic. Next Index: {next_index}")

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
        participants_count = int(quiz_status.get("participant_count", 0))
        category_display_name = quiz_status.get("category_display_name", "عامة")

        full_new_question_message_text = (
            f"❓ {base_question_text_for_redis}\n\n"
            f"🏷️ <b>الفئة</b>: {html.escape(category_display_name)}\n"
            f"👥 <b>المشاركون</b>: {participants_count}\n"
            f"⏳ <b>الوقت المتبقي</b>: {time_per_question} ثانية"
        )

        end_time = datetime.now() + timedelta(seconds=time_per_question)
        bot_token = quiz_status.get("bot_token")
        quiz_identifier = quiz_status.get("quiz_identifier")

        # ## <-- التغيير هنا: إعادة ضبط علامة تحديث منتصف الوقت للسؤال الجديد
        await redis_handler.redis_client.hset(
            quiz_key, mapping={
                "current_question_text": base_question_text_for_redis,
                "current_keyboard": json.dumps(keyboard),
                "current_index": next_index,
                "mid_update_sent": "0"  # إعادة الضبط للسؤال التالي
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
        await end_quiz(quiz_key, quiz_status, telegram_bot)


# ... (دالة end_quiz و main_loop تبقيان كما هما من الإصدار الأخير)
async def end_quiz(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    lock_key = f"Lock:EndQuiz:{quiz_key}"
    if not await redis_handler.redis_client.set(lock_key, "true", ex=60, nx=True):
        logger.warning(f"Worker: [{quiz_key}] End process is already locked. Skipping.")
        return
    try:
        logger.info(f"Worker: [{quiz_key}] Starting end_quiz process (lock acquired).")
        bot_token = quiz_status.get("bot_token")
        quiz_identifier = quiz_status.get("quiz_identifier")
        stats_db_path = quiz_status.get("stats_db_path")
        signature_text = quiz_status.get("signature_text", "بـوت تـحدي الاسئلة ❓ (https://t.me/nniirrbot)")
        if not bot_token or not quiz_identifier:
            logger.error(f"Worker: [{quiz_key}] Cannot end quiz, missing bot_token or quiz_identifier.")
            return
        if not stats_db_path:
            results_text = "🏆 <b>المسابقة انتهت!</b> 🏆\n\nحدث خطأ في حفظ النتائج. يرجى مراجعة السجلات."
            message_data = {"text": results_text, "reply_markup": json.dumps({}), "parse_mode": "HTML"}
            await _send_telegram_update(quiz_key, telegram_bot, message_data, quiz_status)
            await redis_handler.end_quiz(bot_token, quiz_identifier)
            return
        total_questions = len(json.loads(quiz_status.get("question_ids", "[]")))
        logger.info(f"Worker: [{quiz_key}] Calculating results from Redis.")
        final_scores = {}
        total_participants_who_answered = 0
        async for key in redis_handler.redis_client.scan_iter(f"QuizAnswers:{bot_token}:{quiz_identifier}:*"):
            try:
                user_id = int(key.split(":")[-1])
                user_data = await redis_handler.redis_client.hgetall(key)
                score = int(user_data.get('score', 0))
                username = html.escape(user_data.get('username', f"User_{user_id}"))
                user_answers = {int(k.split('.')[1]): int(v) for k, v in user_data.items() if k.startswith('answers.')}
                final_scores[user_id] = {'score': score, 'username': username, 'answers': user_answers}
                if len(user_answers) > 0: total_participants_who_answered += 1
            except (ValueError, IndexError) as e: logger.warning(f"Worker: [{quiz_key}] Could not parse user data from answer key '{key}': {e}")
        registered_players_json = quiz_status.get('players', '[]')
        try: total_registered_players = len(json.loads(registered_players_json))
        except json.JSONDecodeError: logger.warning(f"Worker: [{quiz_key}] Could not decode 'players' JSON."); total_registered_players = 0
        not_answered_count = max(0, total_registered_players - total_participants_who_answered)
        sorted_participants = sorted(final_scores.items(), key=lambda item: item[1]['score'], reverse=True)
        winner_id, winner_score, winner_username_escaped = (None, 0, "لا يوجد")
        if sorted_participants:
            winner_id, winner_data = sorted_participants[0]
            winner_score = winner_data['score']
            try:
                get_user_info = await telegram_bot.get_chat_member(chat_id=winner_id, user_id=winner_id)
                if get_user_info and get_user_info.get("ok"):
                    user_api_data = get_user_info.get("result", {})
                    if user_api_data.get("username"): winner_username_escaped = f"@{html.escape(user_api_data['username'])}"
                    elif user_api_data.get("first_name"): winner_username_escaped = f"<a href='tg://user?id={user_api_data['id']}'>{user_api_data['first_name']}</a>"
                    else: winner_username_escaped = winner_data.get('username', f"User_{winner_id}")
                else: winner_username_escaped = winner_data.get('username', f"User_{winner_id}")
            except Exception as e: logger.warning(f"Worker: [{quiz_key}] Could not fetch winner info: {e}"); winner_username_escaped = winner_data.get('username', f"User_{winner_id}")
        ltr, pdf = '\u202A', '\u202C'
        results_text = "🏆 <b>المسابقة انتهت! النتائج النهائية:</b> 🏆\n\n"
        if winner_id: results_text += f"🎉 <b>الفائز</b>: {ltr}{winner_username_escaped}{pdf} بـ {winner_score} نقطة!\n\n"
        else: results_text += "😞 لم يشارك أحد في المسابقة أو لم يحصل أحد على نقاط.\n\n"
        results_text += f"{BLOCKQUOTE_OPEN_TAG}"
        results_text += f"📊 <b>إحصائيات المشاركة:</b>\n"
        results_text += f"• إجمالي اللاعبين المسجلين: {total_registered_players}\n"
        results_text += f"• عدد من شارك بإجابات: {total_participants_who_answered}\n"
        results_text += f"• عدد لم يشارك بإجابات: {not_answered_count}\n\n"
        results_text += f"{BLOCKQUOTE_CLOSE_TAG}"
        if len(sorted_participants) > 0:
            results_text += f"🏅 <b>لوحة المتصدرين:</b>\n"
            leaderboard_content = ""
            for i, (user_id, data) in enumerate(sorted_participants[:30]):
                rank_emoji = ""
                if i == 0: rank_emoji = "🥇 "
                elif i == 1: rank_emoji = "🥈 "
                elif i == 2: rank_emoji = "🥉 "
                leaderboard_content += f"{rank_emoji}{ltr}{i+1}. {data['username']}: {data['score']}{pdf} نقطة\n"
            results_text += f"{BLOCKQUOTE_OPEN_TAG}\n{leaderboard_content}{BLOCKQUOTE_CLOSE_TAG}\n"
        else: results_text += "😔 لا توجد نتائج لعرضها.\n"
        if signature_text: results_text += f"\n{signature_text}"
        try:
            logger.info(f"Worker: [{quiz_key}] Saving quiz history and stats to SQLite: {stats_db_path}")
            quiz_history_id = await sqlite_handler.save_quiz_history(stats_db_path, quiz_identifier, total_questions, winner_id, winner_score)
            for user_id, data in final_scores.items():
                total_points = data['score']
                username_for_db = data['username']
                correct_answers_count = sum(1 for q_score in data['answers'].values() if q_score > 0)
                total_answered_questions_count = len(data['answers'])
                wrong_answers_count = total_answered_questions_count - correct_answers_count
                await sqlite_handler.update_user_stats(stats_db_path, user_id, username_for_db, total_points, correct_answers_count, wrong_answers_count)
                await sqlite_handler.save_quiz_participant(stats_db_path, quiz_history_id, user_id, total_points, data['answers'])
            logger.info(f"Worker: [{quiz_key}] Quiz results saved to SQLite successfully.")
        except Exception as e:
            logger.error(f"Worker: [{quiz_key}] Failed to save quiz results to SQLite: {e}", exc_info=True)
            await send_admin_notification(bot_token, f"<b>خطأ في حفظ نتائج المسابقة:</b> {quiz_key}\nالسبب: {html.escape(str(e))}\nالرجاء التحقق من SQLite.")
        message_data = {"text": results_text, "reply_markup": json.dumps({}), 'disable_web_page_preview': True, "parse_mode": "HTML"}
        await _send_telegram_update(quiz_key, telegram_bot, message_data, quiz_status)
        logger.info(f"Worker: [{quiz_key}] Final results message sent.")
    finally:
        await redis_handler.end_quiz(bot_token, quiz_identifier)
        await redis_handler.redis_client.delete(lock_key)
        logger.info(f"Worker: [{quiz_key}] Quiz cleaned up from Redis and lock released.")


async def main_loop():
    logger.info("Worker: Starting main loop...")
    ignore_keywords = [":askquestion", ":newpost", ":Newpost", ":stats", ":leaderboard", ":start", "Panel"]
    while True:
        try:
            all_quiz_keys = [key async for key in redis_handler.redis_client.scan_iter("Quiz:*:*")]
            active_quiz_keys_to_process = [key for key in all_quiz_keys if not any(keyword in key for keyword in ignore_keywords)]
            if active_quiz_keys_to_process:
                tasks = [process_active_quiz(key) for key in active_quiz_keys_to_process]
                await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Worker: An critical error occurred in the main loop: {e}", exc_info=True)

        await asyncio.sleep(0.2)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Worker: Shutting down gracefully.")
    except Exception as e:
        logger.error(f"Worker: Fatal error in worker startup/main: {e}", exc_info=True)