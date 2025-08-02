import asyncio
import json
from datetime import datetime, timedelta
import logging
import os
import html # <--- تم إضافة هذا الاستيراد

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

def get_telegram_bot(token: str) -> TelegramBotServiceAsync:
    if token not in bot_instances:
        bot_instances[token] = TelegramBotServiceAsync(token)
    return bot_instances[token]

async def update_question_display(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync, time_left: float, force_update: bool = False):
    UPDATE_INTERVAL_SECONDS = 5
    now = datetime.now()

    if not force_update:
        last_update_str = await redis_handler.redis_client.hget(quiz_key, "last_display_update")
        if last_update_str:
            try:
                last_update_time = datetime.fromisoformat(last_update_str)
                if (now - last_update_time).total_seconds() < UPDATE_INTERVAL_SECONDS:
                    logger.debug(f"Worker: [{quiz_key}] Display update skipped due to rate limiting.")
                    return
            except ValueError:
                logger.warning(f"Worker: [{quiz_key}] Could not parse last_display_update timestamp: {last_update_str}")

    await redis_handler.redis_client.hset(quiz_key, "last_display_update", now.isoformat())
    logger.info(f"Worker: [{quiz_key}] Proceeding with display update (force_update={force_update}).")

    bot_token = quiz_status.get("bot_token")
    quiz_identifier = quiz_status.get("quiz_identifier")
    inline_message_id = quiz_status.get("inline_message_id")
    chat_id = quiz_status.get("chat_id")
    message_id = quiz_status.get("message_id")

    # تطبيق html.escape على النص الأساسي للسؤال
    base_question_text_from_redis = html.escape(quiz_status.get("current_question_text", ""))

    if not all([bot_token, quiz_identifier, base_question_text_from_redis]):
        logger.warning(f"Worker: [{quiz_key}] Missing core data (token, identifier, or base_question_text). Skipping.")
        return
    if not (inline_message_id or (chat_id and message_id)):
        logger.warning(f"Worker: [{quiz_key}] Missing message identifiers (inline_message_id OR chat_id/message_id). Skipping.")
        return

    try:
        participant_keys = [key async for key in redis_handler.redis_client.scan_iter(f"QuizAnswers:{bot_token}:{quiz_identifier}:*")]
        participants = len(participant_keys)
    except Exception as e:
        logger.error(f"Worker: [{quiz_key}] Failed to scan for participants: {e}", exc_info=True)
        return

    new_text = (
        f"❓ {base_question_text_from_redis}\n\n"
        f"👥 <b>المشاركون</b>: {participants}\n" # <--- استخدام <b> بدلاً من **
        f"⏳ <b>الوقت المتبقي</b>: {int(time_left)} ثانية" # <--- استخدام <b> بدلاً من **
    )

    current_keyboard_str = quiz_status.get("current_keyboard")
    if not current_keyboard_str:
        logger.warning(f"Worker: [{quiz_key}] 'current_keyboard' not found in status. Cannot update message without it.")
        return

    message_data = {
        "text": new_text,
        "reply_markup": current_keyboard_str,
        "parse_mode": "HTML" # <--- تم التغيير إلى HTML
    }

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
            logger.error(f"Worker: [{quiz_key}] No valid message identifier for editing.")
            return

        # حل مشكلة 'bool' object has no attribute 'get'
        if response.get("ok") and response.get("result") is True:
            logger.debug(f"Worker: [{quiz_key}] Telegram reported message edited successfully (result=True), no new IDs to update.")
        elif not response.get("ok"):
            if "message is not modified" not in response.get("description", ""):
                logger.error(f"Worker: [{quiz_key}] Telegram reported failure to update display: {response.get('description')}")
        else:
            logger.debug(f"Worker: [{quiz_key}] Successfully updated display message.")

        # تحديث معرفات الرسالة في Redis بعد كل تحديث للرسالة
        if response.get("result") and isinstance(response.get("result"), dict): # التحقق من أن 'result' هو قاموس
            updated_inline_message_id = response["result"].get("inline_message_id")
            updated_chat_id = response["result"].get("chat", {}).get("id")
            updated_message_id = response["result"].get("message_id")

            if updated_inline_message_id and quiz_status.get("inline_message_id") != updated_inline_message_id:
                await redis_handler.redis_client.hset(quiz_key, "inline_message_id", updated_inline_message_id)
                logger.debug(f"Worker: [{quiz_key}] Updated inline_message_id in Redis: {updated_inline_message_id}")
            elif updated_chat_id and updated_message_id:
                if quiz_status.get("chat_id") != str(updated_chat_id) or quiz_status.get("message_id") != str(updated_message_id):
                    await redis_handler.redis_client.hset(quiz_key, "chat_id", str(updated_chat_id))
                    await redis_handler.redis_client.hset(quiz_key, "message_id", str(updated_message_id))
                    logger.debug(f"Worker: [{quiz_key}] Updated chat_id/message_id in Redis: {updated_chat_id}/{updated_message_id}")

    except asyncio.TimeoutError:
        logger.warning(f"Worker: [{quiz_key}] Timed out while trying to update display message.")
    except Exception as e:
        logger.error(f"Worker: [{quiz_key}] Failed to update display message due to an exception: {e}", exc_info=True)


async def process_active_quiz(quiz_key: str):
    logger.info(f"Worker: Processing quiz key: {quiz_key}")
    quiz_status = await redis_handler.get_quiz_status_by_key(quiz_key)

    if not quiz_status:
        logger.warning(f"Worker: [{quiz_key}] No status found in Redis. It might have been cleaned up. Skipping.")
        return

    if quiz_status.get("status") == "stopping":
        logger.info(f"Worker: [{quiz_key}] Found in 'stopping' state. Attempting to finalize.")
        bot_token = quiz_status.get("bot_token")
        quiz_identifier = quiz_status.get("quiz_identifier")
        if bot_token and quiz_identifier:
            await end_quiz(quiz_key, quiz_status, get_telegram_bot(bot_token))
        else:
            logger.error(f"Worker: [{quiz_key}] Cannot finalize 'stopping' quiz, bot_token or quiz_identifier is missing.")
        return

    if quiz_status.get("status") not in ["active", "initializing"]:
        logger.info(f"Worker: [{quiz_key}] Status is not 'active' or 'initializing' (it's '{quiz_status.get('status')}'). Skipping question progression.")
        return

    bot_token = quiz_status.get("bot_token")
    quiz_identifier = quiz_status.get("quiz_identifier")
    if not bot_token or not quiz_identifier:
        logger.error(f"Worker: [{quiz_key}] Bot token or quiz_identifier not found in status. Cleaning up broken state.")
        await redis_handler.redis_client.delete(quiz_key)
        return

    telegram_bot = get_telegram_bot(bot_token)
    quiz_time_key = redis_handler.quiz_time_key(bot_token, quiz_identifier)
    quiz_time = await redis_handler.redis_client.hgetall(quiz_time_key)

    logger.debug(f"Worker: [{quiz_key}] Full Status: {quiz_status}")
    logger.debug(f"Worker: [{quiz_key}] Timing Info: {quiz_time}")

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
        logger.info(f"Worker: [{quiz_key}] No active question timer found. Assuming it's time for the next question.")
        should_process_next_question = True

    if should_process_next_question:
        await handle_next_question(quiz_key, quiz_status, telegram_bot)

async def handle_next_question(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
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
            logger.error(f"Worker: [{quiz_key}] 'questions_db_path' missing in quiz status. Cannot fetch question. Ending quiz.")
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        question = await sqlite_handler.get_question_by_id(questions_db_path, next_question_id)
        if not question:
            logger.error(f"Worker: [{quiz_key}] Question ID {next_question_id} not found in DB '{questions_db_path}'. Ending quiz.")
            await end_quiz(quiz_key, quiz_status, telegram_bot)
            return

        # تطبيق html.escape على نص السؤال
        base_question_text_for_redis = f"<b>السؤال {next_index + 1}</b>: {html.escape(question['question'])}" # <--- استخدام <b> و html.escape

        options = [html.escape(question['opt1']), html.escape(question['opt2']), html.escape(question['opt3']), html.escape(question['opt4'])] # <--- html.escape على الخيارات
        # تم تعديل هذا السطر لتضمين quiz_game_id (quiz_identifier) في callback_data
        quiz_identifier_for_callbacks = quiz_status.get("quiz_identifier")
        keyboard = {"inline_keyboard": [[{"text": opt, "callback_data": f"answer_{quiz_identifier_for_callbacks}_{next_question_id}_{i}"}] for i, opt in enumerate(options)]}

        time_per_question = int(quiz_status.get("time_per_question", 30))
        initial_participants_count_for_new_q = 0
        initial_time_display_for_new_q = time_per_question
        full_new_question_message_text = (
            f"❓ {base_question_text_for_redis}\n\n"
            f"👥 <b>المشاركون</b>: {initial_participants_count_for_new_q}\n" # <--- استخدام <b>
            f"⏳ <b>الوقت المتبقي</b>: {initial_time_display_for_new_q} ثانية" # <--- استخدام <b>
        )

        inline_message_id = quiz_status.get("inline_message_id")
        chat_id = quiz_status.get("chat_id")
        message_id = quiz_status.get("message_id")

        message_data = {
            "text": full_new_question_message_text,
            "reply_markup": json.dumps(keyboard),
            "parse_mode": "HTML" # <--- تم التغيير إلى HTML
        }

        logger.info(f"Worker: [{quiz_key}] Attempting to edit message for Q{next_index + 1} (ID: {next_question_id}).")
        try:
            response = None
            if inline_message_id:
                message_data["inline_message_id"] = inline_message_id
                response = await telegram_bot.edit_inline_message(message_data)
            elif chat_id and message_id:
                message_data["chat_id"] = chat_id
                message_data["message_id"] = message_id
                response = await telegram_bot.edit_message(message_data)
            else:
                logger.error(f"Worker: [{quiz_key}] No valid message identifier to edit for next question. Ending quiz.")
                await end_quiz(quiz_key, quiz_status, telegram_bot)
                return

            logger.info(f"Worker: [{quiz_key}] Telegram API response for edit_message: {response}")
            # حل مشكلة 'bool' object has no attribute 'get'
            if response.get("ok") and response.get("result") is True:
                logger.debug(f"Worker: [{quiz_key}] Telegram reported message edited successfully (result=True), no new IDs to update.")
            elif not response.get("ok"):
                if "message is not modified" not in response.get("description", ""):
                    logger.error(f"Worker: [{quiz_key}] Telegram reported failure to edit message: {response.get('description')}. Ending quiz.")
                    await end_quiz(quiz_key, quiz_status, telegram_bot)
                    return
            logger.info(f"Worker: [{quiz_key}] Question {next_index + 1} (ID: {next_question_id}) sent/edited successfully.")

            # تحديث معرفات الرسالة في Redis بعد كل تحديث للرسالة
            if response.get("result") and isinstance(response.get("result"), dict): # التحقق من أن 'result' هو قاموس
                updated_inline_message_id = response["result"].get("inline_message_id")
                updated_chat_id = response["result"].get("chat", {}).get("id")
                updated_message_id = response["result"].get("message_id")

                if updated_inline_message_id and quiz_status.get("inline_message_id") != updated_inline_message_id:
                    await redis_handler.redis_client.hset(quiz_key, "inline_message_id", updated_inline_message_id)
                    logger.debug(f"Worker: [{quiz_key}] Updated inline_message_id in Redis: {updated_inline_message_id}")
                elif updated_chat_id and updated_message_id:
                    if quiz_status.get("chat_id") != str(updated_chat_id) or quiz_status.get("message_id") != str(updated_message_id):
                        await redis_handler.redis_client.hset(quiz_key, "chat_id", str(updated_chat_id))
                        await redis_handler.redis_client.hset(quiz_key, "message_id", str(updated_message_id))
                        logger.debug(f"Worker: [{quiz_key}] Updated chat_id/message_id in Redis: {updated_chat_id}/{updated_message_id}")

        except asyncio.TimeoutError:
            logger.warning(f"Worker: [{quiz_key}] Timed out while trying to update display message.")
        except Exception as e:
            logger.error(f"Worker: [{quiz_key}] Failed to update display message due to an exception: {e}", exc_info=True)

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

        logger.info(f"Worker: [{quiz_key}] Performing initial display update for new question.")
        refreshed_quiz_status = await redis_handler.get_quiz_status_by_key(quiz_key)
        await update_question_display(quiz_key, refreshed_quiz_status, telegram_bot, time_per_question, force_update=True)

        logger.info(f"Worker: [{quiz_key}] State updated. New current_index: {next_index}. Timer set for {time_per_question}s.")

    else:
        logger.info(f"Worker: [{quiz_key}] End of questions reached. Finishing up.")
        await end_quiz(quiz_key, quiz_status, telegram_bot)


async def end_quiz(quiz_key: str, quiz_status: dict, telegram_bot: TelegramBotServiceAsync):
    lock_key = f"Lock:EndQuiz:{quiz_key}"
    if not await redis_handler.redis_client.set(lock_key, "true", ex=60, nx=True):
        logger.warning(f"Worker: [{quiz_key}] End process is already locked or in progress. Skipping to prevent loop.")
        return

    logger.info(f"Worker: [{quiz_key}] Starting end_quiz process (lock acquired).")

    bot_token = quiz_status.get("bot_token")
    quiz_identifier = quiz_status.get("quiz_identifier")
    if not bot_token or not quiz_identifier:
        logger.error(f"Worker: [{quiz_key}] Cannot end quiz, bot_token or quiz_identifier is missing. Releasing lock and exiting.")
        await redis_handler.redis_client.delete(lock_key)
        return

    inline_message_id = quiz_status.get("inline_message_id")
    chat_id = quiz_status.get("chat_id")
    message_id = quiz_status.get("message_id")
    stats_db_path = quiz_status.get("stats_db_path")

    if not stats_db_path:
        logger.error(f"Worker: [{quiz_key}] 'stats_db_path' not found in quiz status. Cannot save results to SQLite.")
        results_text = "🏆 <b>المسابقة انتهت!</b> 🏆\n\nحدث خطأ في حفظ النتائج. يرجى مراجعة سجلات الخادم." # <--- استخدام <b>
        message_data = {"text": results_text, "reply_markup": json.dumps({}), "parse_mode": "HTML"} # <--- تم التغيير إلى HTML
        try:
            if inline_message_id:
                message_data["inline_message_id"] = inline_message_id
                await telegram_bot.edit_inline_message(message_data)
            elif chat_id and message_id:
                message_data["chat_id"] = chat_id
                message_data["message_id"] = message_id
                await telegram_bot.edit_message(message_data)
            logger.info(f"Worker: [{quiz_key}] Error message sent to Telegram due to missing stats_db_path.")
        except Exception as e:
            logger.error(f"Worker: [{quiz_key}] Failed to send error message to Telegram: {e}", exc_info=True)
        await redis_handler.end_quiz(bot_token, quiz_identifier)
        await redis_handler.redis_client.delete(lock_key)
        return

    total_questions = len(json.loads(quiz_status.get("question_ids", "[]")))

    logger.info(f"Worker: [{quiz_key}] Calculating results from Redis.")
    final_scores = {}
    async for key in redis_handler.redis_client.scan_iter(f"QuizAnswers:{bot_token}:{quiz_identifier}:*"):
        try:
            user_id = int(key.split(":")[-1])
            user_data = await redis_handler.redis_client.hgetall(key)
            score = int(user_data.get('score', 0))
            # تطبيق html.escape على اسم المستخدم
            username = html.escape(user_data.get('username', f"User_{user_id}")) # <--- html.escape
            if username.startswith("User_"): # إذا لم يتم العثور على اسم مستخدم، لا تقم بتوسيع "User_..." باسم مستخدم حقيقي
                pass # لا داعي لـ html.escape هنا مرة أخرى

            user_answers = {}
            for k, v in user_data.items():
                if k.startswith('answers.'):
                    try:
                        q_id = int(k.split('.')[1])
                        user_answers[q_id] = int(v)
                    except ValueError:
                        logger.warning(f"Worker: [{quiz_key}] Malformed answer key/value for user {user_id}, key {k}: {v}")

            final_scores[user_id] = {'score': score, 'username': username, 'answers': user_answers}
            logger.debug(f"Worker: [{quiz_key}] Collected results for user {user_id}: score={score}, username={username}")
        except (ValueError, IndexError) as e:
            logger.warning(f"Worker: [{quiz_key}] Could not parse user data from answer key '{key}': {e}")
            continue

    sorted_participants = sorted(final_scores.items(), key=lambda item: item[1]['score'], reverse=True)
    winner_id, winner_score, winner_username_escaped = (None, 0, "لا يوجد")
    if sorted_participants:
        winner_id, winner_data = sorted_participants[0]
        winner_score, winner_username_escaped = winner_data['score'], winner_data['username'] # هنا `username` بالفعل تم عمل escape له

    # رموز التحكم بالاتجاه
    ltr = '\u202A'  # Left-to-right embedding
    pdf = '\u202C'  # Pop directional formatting

    results_text = "🏆 <b>المسابقة انتهت! النتائج النهائية:</b> 🏆\n\n" # <--- استخدام <b>
    if winner_id:
        # استخدام رموز الاتجاه لضمان الاتجاه الصحيح
        results_text += f"🎉 <b>الفائز</b>: {ltr}{winner_username_escaped}{pdf} بـ {winner_score} نقطة!\n\n" # <--- استخدام <b> و ltr/pdf
    else:
        results_text += "😞 لم يشارك أحد في المسابقة أو لم يحصل أحد على نقاط.\n\n"

    if len(sorted_participants) > 0:
        results_text += "🏅 <b>لوحة المتصدرين:</b>\n" # <--- استخدام <b>
        for i, (user_id, data) in enumerate(sorted_participants[:10]):
            rank_emoji = ""
            if i == 0: rank_emoji = "🥇 "
            elif i == 1: rank_emoji = "🥈 "
            elif i == 2: rank_emoji = "🥉 "

            # تطبيق رموز الاتجاه هنا: نجبر الكتلة الأولى (الرقم والاسم والنقاط) على أن تكون LTR
            # ثم يأتي النص العربي "نقطة" بعدها بشكل طبيعي
            results_text += f"{rank_emoji}{ltr}{i+1}. {data['username']}: {data['score']}{pdf} نقطة\n"
    else:
        results_text += "😔 لا توجد نتائج لعرضها.\n"

    try:
        logger.info(f"Worker: [{quiz_key}] Saving quiz history and updating user stats in SQLite DB: {stats_db_path}")
        # هنا قد تحتاج إلى التأكد من أن حقل quiz_identifier موجود في جدول quiz_history
        # يمكنك إضافة هذا السطر في ملف sqlite_handler.py في دالة إنشاء الجدول:
        # CREATE TABLE IF NOT EXISTS quiz_history ( ... quiz_identifier TEXT, ... )
        quiz_history_id = await sqlite_handler.save_quiz_history(stats_db_path, quiz_identifier, total_questions, winner_id, winner_score)

        for user_id, data in final_scores.items():
            total_points = data['score']
            username = data['username'] # هنا `username` هو الاسم الذي تم عمل escape له بالفعل
            correct_answers_count = sum(1 for q_score in data['answers'].values() if q_score > 0)
            total_answered_questions_count = len(data['answers'])
            wrong_answers_count = total_answered_questions_count - correct_answers_count

            await sqlite_handler.update_user_stats(stats_db_path, user_id, username, total_points, correct_answers_count, wrong_answers_count)
            await sqlite_handler.save_quiz_participant(stats_db_path, quiz_history_id, user_id, total_points, data['answers'])

        logger.info(f"Worker: [{quiz_key}] Quiz results saved to SQLite successfully.")
    except Exception as e:
        logger.error(f"Worker: [{quiz_key}] Failed to save quiz results to SQLite: {e}", exc_info=True)

    message_data = {"text": results_text, "reply_markup": json.dumps({}), "parse_mode": "HTML"} # <--- تم التغيير إلى HTML
    try:
        if inline_message_id:
            message_data["inline_message_id"] = inline_message_id
            await telegram_bot.edit_inline_message(message_data)
        elif chat_id and message_id:
            message_data["chat_id"] = chat_id
            message_data["message_id"] = message_id
            await telegram_bot.edit_message(message_data)
        logger.info(f"Worker: [{quiz_key}] Final results message sent to Telegram.")
    except Exception as e:
        logger.error(f"Worker: [{quiz_key}] Failed to send final message to Telegram: {e}", exc_info=True)

    await redis_handler.end_quiz(bot_token, quiz_identifier)
    await redis_handler.redis_client.delete(lock_key)
    logger.info(f"Worker: [{quiz_key}] Quiz has ended and been cleaned up from Redis, including the lock.")


async def main_loop():
    logger.info("Worker: Starting main loop...")
    # Define the keywords to ignore
    ignore_keywords = [
        ":askquestion",
        ":newpost",
        ":Newpost",
        ":stats",
        ":leaderboard",
        ":start"
    ]

    while True:
        try:
            # Get all quiz keys
            all_quiz_keys = [key async for key in redis_handler.redis_client.scan_iter("Quiz:*:*")]

            # Filter out keys containing the ignore keywords
            active_quiz_keys_to_process = [
                key for key in all_quiz_keys
                if not any(keyword in key for keyword in ignore_keywords)
            ]

            if active_quiz_keys_to_process:
                logger.info(f"Worker: Found {len(active_quiz_keys_to_process)} quiz keys to process: {active_quiz_keys_to_process}")
                tasks = [process_active_quiz(key) for key in active_quiz_keys_to_process]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Worker: An error occurred while processing quiz {active_quiz_keys_to_process[i]}: {result}", exc_info=result)
            else:
                logger.debug("Worker: No active quizzes (after filtering) found. Waiting...")

        except Exception as e:
            logger.error(f"Worker: An critical error occurred in the main loop: {e}", exc_info=True)

        await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Worker: Shutting down gracefully.")
    except Exception as e:
        logger.error(f"Worker: Fatal error in worker startup/main: {e}", exc_info=True)