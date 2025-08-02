import asyncio
import json
from datetime import datetime, timedelta
import logging
import os

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

    base_question_text_from_redis = quiz_status.get("current_question_text", "")

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
        f"👥 **المشاركون**: {participants}\n"
        f"⏳ **الوقت المتبقي**: {int(time_left)} ثانية"
    )

    current_keyboard_str = quiz_status.get("current_keyboard")
    if not current_keyboard_str:
        logger.warning(f"Worker: [{quiz_key}] 'current_keyboard' not found in status. Cannot update message without it.")
        return

    message_data = {
        "text": new_text,
        "reply_markup": current_keyboard_str,
        "parse_mode": "Markdown"
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

        # --- START MODIFIED CODE BLOCK (for AttributeError) ---
        # فحص نوع الاستجابة قبل محاولة الوصول إلى خصائصها
        if not isinstance(response, dict):
            logger.error(f"Worker: [{quiz_key}] Telegram API call returned unexpected type: {type(response)} with value {response}. Expected dict.")
            if response is True: # إذا أعادت API تيليجرام True (نجاح بدون تفاصيل JSON)
                logger.debug(f"Worker: [{quiz_key}] Telegram API call assumed successful (returned True). Skipping detailed result parsing and message ID update.")
                return # نخرج من الدالة بعد افتراض النجاح
            else: # إذا أعادت False أو None أو أي نوع غير متوقع
                logger.error(f"Worker: [{quiz_key}] Telegram API call failed or returned unexpected non-dict value.")
                # في هذه الحالة، لا يمكننا تأكيد التحديث وقد تحتاج المسابقة إلى الإنهاء إذا كانت هذه مشكلة حرجة
                # ولكن من الأفضل أن نستمر ونترك logic المعالجة يكتشف ما إذا كانت المسابقة عالقة.
                return # نخرج من الدالة لمنع المزيد من الأخطاء
        # --- END MODIFIED CODE BLOCK ---

        if not response.get("ok"):
            if "message is not modified" not in response.get("description", ""):
                logger.error(f"Worker: [{quiz_key}] Telegram reported failure to update display: {response.get('description')}")
        else:
            logger.debug(f"Worker: [{quiz_key}] Successfully updated display message.")

            # --- تحديث معرفات الرسالة في Redis بعد كل تحديث للرسالة ---
            if response.get("result"):
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

    # هذا الفحص يتم تنفيذه الآن بشكل أكثر دقة في main_loop
    # ولكن يمكن تركه هنا كطبقة أمان إضافية
    if not quiz_status:
        logger.warning(f"Worker: [{quiz_key}] No status found in Redis after being passed to process_active_quiz. Skipping.")
        return

    if quiz_status.get("status") == "stopping":
        logger.info(f"Worker: [{quiz_key}] Found in 'stopping' state. Attempting to finalize.")
        bot_token = quiz_status.get("bot_token")
        quiz_identifier = quiz_status.get("quiz_identifier")
        if bot_token and quiz_identifier:
            await end_quiz(quiz_key, quiz_status, get_telegram_bot(bot_token))
        else:
            logger.error(f"Worker: [{quiz_key}] Cannot finalize 'stopping' quiz, bot_token or quiz_identifier is missing in quiz_status.")
            await redis_handler.redis_client.delete(quiz_key) # Clean up broken stopping state
        return

    if quiz_status.get("status") not in ["active", "initializing", "pending"]: # تم إضافة "pending"
        logger.info(f"Worker: [{quiz_key}] Status is not 'active', 'initializing', or 'pending' (it's '{quiz_status.get('status')}'). Skipping question progression.")
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

    # إذا كانت المسابقة في حالة "pending" (انتظار البدء)، يجب أن نبدأها
    if quiz_status.get("status") == "pending":
        # هذه الحالة يجب أن تعالجها الـ API عند استدعاء /start_competition
        # ولكن كطبقة حماية، إذا وصل هنا وكانت pending ولم يتم إعداد المؤقت، فاعتبره جاهزًا للبدء
        if not quiz_time.get("start") and not quiz_time.get("end"):
            logger.info(f"Worker: [{quiz_key}] Quiz in 'pending' state without timer. Assuming ready to start (or re-start initialization).")
            # لا نفعل شيئًا هنا، نتوقع أن الـ API هي التي سترسل أول سؤال
            # أو يمكننا إضافة منطق لجعل العامل يحفز إرسال أول سؤال إذا لم يحدث
            # حالياً، نترك الـ API تقوم بذلك. العامل سيتجاهلها حتى تتحول لـ 'active'
            # ولكن إذا كان قد بدأ بالفعل وكان هذا مجرد تنظيف، فسننتقل إلى السؤال التالي
            # أفضل طريقة هي معالجة 'pending' في الـ API فقط عند طلب بدء المسابقة
            # هنا، إذا كانت 'pending' فقط، لا تفعل شيئاً إلا إذا كان هناك مؤقت فعلي.
            pass
        else: # إذا كانت pending ولها مؤقت، فهذا يعني أنها بدأت بالفعل وتحولت إلى active
            logger.info(f"Worker: [{quiz_key}] Quiz in 'pending' state but has a timer set. Forcing status to 'active'.")
            await redis_handler.redis_client.hset(quiz_key, "status", "active")
            # ونستمر في منطق المؤقت أدناه
            pass


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
        # إذا لم يكن هناك مؤقت للسؤال الحالي، فهذا يعني أنه حان الوقت للانتقال إلى السؤال التالي
        # أو أن المسابقة بدأت للتو وتحتاج إلى إعداد أول سؤال
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

        base_question_text_for_redis = f"**السؤال {next_index + 1}**: {question['question']}"

        options = [question['opt1'], question['opt2'], question['opt3'], question['opt4']]
        quiz_identifier_for_callbacks = quiz_status.get("quiz_identifier")
        keyboard = {"inline_keyboard": [[{"text": opt, "callback_data": f"answer_{quiz_identifier_for_callbacks}_{next_question_id}_{i}"}] for i, opt in enumerate(options)]}

        time_per_question = int(quiz_status.get("time_per_question", 30))
        initial_participants_count_for_new_q = 0 # سيتم تحديث هذا من خلال update_question_display
        initial_time_display_for_new_q = time_per_question
        full_new_question_message_text = (
            f"❓ {base_question_text_for_redis}\n\n"
            f"👥 **المشاركون**: {initial_participants_count_for_new_q}\n"
            f"⏳ **الوقت المتبقي**: {initial_time_display_for_new_q} ثانية"
        )

        inline_message_id = quiz_status.get("inline_message_id")
        chat_id = quiz_status.get("chat_id")
        message_id = quiz_status.get("message_id")

        message_data = {
            "text": full_new_question_message_text,
            "reply_markup": json.dumps(keyboard),
            "parse_mode": "Markdown"
        }

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

            # --- START MODIFIED CODE BLOCK (for AttributeError) ---
            if not isinstance(response, dict):
                logger.error(f"Worker: [{quiz_key}] Telegram API call returned unexpected type: {type(response)} with value {response}. Expected dict. For edit_message (next question).")
                if response is True:
                    logger.debug(f"Worker: [{quiz_key}] Telegram API call assumed successful (returned True). Skipping detailed result parsing.")
                else:
                    logger.error(f"Worker: [{quiz_key}] Telegram API call failed or returned unexpected non-dict value. Ending quiz for critical message update failure.")
                    await end_quiz(quiz_key, quiz_status, telegram_bot)
                return # نخرج من الدالة بعد معالجة الاستجابة غير المتوقعة
            # --- END MODIFIED CODE BLOCK ---

            logger.info(f"Worker: [{quiz_key}] Telegram API response for edit_message (Q{next_index + 1}): {response}")
            if not response.get("ok"):
                if "message is not modified" not in response.get("description", ""):
                    logger.error(f"Worker: [{quiz_key}] Telegram reported failure to edit message: {response.get('description')}. Ending quiz.")
                    await end_quiz(quiz_key, quiz_status, telegram_bot)
                    return
            logger.info(f"Worker: [{quiz_key}] Question {next_index + 1} (ID: {next_question_id}) sent/edited successfully.")

            # --- تحديث معرفات الرسالة في Redis بعد كل تحديث للرسالة ---
            if response.get("result"):
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

        # تأكد من أن bot_token و quiz_identifier موجودان في quiz_status قبل استخدامها
        # (يفترض أن هذا قد تم التحقق منه بالفعل في بداية process_active_quiz)
        _bot_token = quiz_status.get("bot_token")
        _quiz_identifier = quiz_status.get("quiz_identifier")

        await redis_handler.set_current_question(_bot_token, _quiz_identifier, next_question_id, end_time)
        await redis_handler.redis_client.hset(
            quiz_key, mapping={
                "current_question_text": base_question_text_for_redis,
                "current_keyboard": json.dumps(keyboard),
                "current_index": next_index,
                "status": "active" # تأكيد أن الحالة نشطة بعد إرسال السؤال
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
        logger.error(f"Worker: [{quiz_key}] Cannot end quiz, bot_token or quiz_identifier is missing from quiz_status. Releasing lock and exiting.")
        await redis_handler.redis_client.delete(lock_key)
        return

    inline_message_id = quiz_status.get("inline_message_id")
    chat_id = quiz_status.get("chat_id")
    message_id = quiz_status.get("message_id")
    stats_db_path = quiz_status.get("stats_db_path")

    if not stats_db_path:
        logger.error(f"Worker: [{quiz_key}] 'stats_db_path' not found in quiz status. Cannot save results to SQLite.")
        results_text = "🏆 **المسابقة انتهت!** 🏆\n\nحدث خطأ في حفظ النتائج. يرجى مراجعة سجلات الخادم."
        message_data = {"text": results_text, "reply_markup": json.dumps({}), "parse_mode": "Markdown"}
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
            username = user_data.get('username', f"User_{user_id}")

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
    winner_id, winner_score, winner_username = (None, 0, "لا يوجد")
    if sorted_participants:
        winner_id, winner_data = sorted_participants[0]
        winner_score, winner_username = winner_data['score'], winner_data['username']

    results_text = "🏆 **المسابقة انتهت! النتائج النهائية:** 🏆\n\n"
    if winner_id:
        results_text += f"🎉 **الفائز**: {winner_username} بـ {winner_score} نقطة!\n\n"
    else:
        results_text += "😞 لم يشارك أحد في المسابقة أو لم يحصل أحد على نقاط.\n\n"

    if len(sorted_participants) > 0:
        results_text += "🏅 **لوحة المتصدرين:**\n"
        for i, (user_id, data) in enumerate(sorted_participants[:10]):
            rank_emoji = ""
            if i == 0: rank_emoji = "🥇 "
            elif i == 1: rank_emoji = "🥈 "
            elif i == 2: rank_emoji = "🥉 "
            results_text += f"{rank_emoji}{i+1}. {data['username']}: {data['score']} نقطة\n"
    else:
        results_text += "😔 لا توجد نتائج لعرضها.\n"

    try:
        logger.info(f"Worker: [{quiz_key}] Saving quiz history and updating user stats in SQLite DB: {stats_db_path}")
        quiz_history_id = await sqlite_handler.save_quiz_history(stats_db_path, quiz_identifier, total_questions, winner_id, winner_score)

        for user_id, data in final_scores.items():
            total_points = data['score']
            username = data['username']
            correct_answers_count = sum(1 for q_score in data['answers'].values() if q_score > 0)
            total_answered_questions_count = len(data['answers'])
            wrong_answers_count = total_answered_questions_count - correct_answers_count

            await sqlite_handler.update_user_stats(stats_db_path, user_id, username, total_points, correct_answers_count, wrong_answers_count)
            await sqlite_handler.save_quiz_participant(stats_db_path, quiz_history_id, user_id, total_points, data['answers'])

        logger.info(f"Worker: [{quiz_key}] Quiz results saved to SQLite successfully.")
    except Exception as e:
        logger.error(f"Worker: [{quiz_key}] Failed to save quiz results to SQLite: {e}", exc_info=True)

    message_data = {"text": results_text, "reply_markup": json.dumps({}), "parse_mode": "Markdown"}
    try:
        if inline_message_id:
            message_data["inline_message_id"] = inline_message_id
            # --- START MODIFIED CODE BLOCK (for AttributeError) ---
            response = await telegram_bot.edit_inline_message(message_data)
            if not isinstance(response, dict):
                logger.error(f"Worker: [{quiz_key}] Telegram API returned unexpected type for end_quiz (inline): {type(response)} with value {response}.")
                # لن نفشل هنا، لأن الرسالة قد تكون قد تم تحديثها بالفعل
            elif not response.get("ok"):
                logger.error(f"Worker: [{quiz_key}] Telegram reported failure to send final message (inline): {response.get('description')}")
            # --- END MODIFIED CODE BLOCK ---
        elif chat_id and message_id:
            message_data["chat_id"] = chat_id
            message_data["message_id"] = message_id
            # --- START MODIFIED CODE BLOCK (for AttributeError) ---
            response = await telegram_bot.edit_message(message_data)
            if not isinstance(response, dict):
                logger.error(f"Worker: [{quiz_key}] Telegram API returned unexpected type for end_quiz (chat): {type(response)} with value {response}.")
            elif not response.get("ok"):
                logger.error(f"Worker: [{quiz_key}] Telegram reported failure to send final message (chat): {response.get('description')}")
            # --- END MODIFIED CODE BLOCK ---
        logger.info(f"Worker: [{quiz_key}] Final results message sent to Telegram.")
    except Exception as e:
        logger.error(f"Worker: [{quiz_key}] Failed to send final message to Telegram: {e}", exc_info=True)

    await redis_handler.end_quiz(bot_token, quiz_identifier)
    await redis_handler.redis_client.delete(lock_key)
    logger.info(f"Worker: [{quiz_key}] Quiz has ended and been cleaned up from Redis, including the lock.")


async def main_loop():
    logger.info("Worker: Starting main loop...")
    while True:
        try:
            # الخطوة 1: جلب جميع المفاتيح التي تبدأ بـ "Quiz:"
            all_quiz_keys_from_redis = [key async for key in redis_handler.redis_client.scan_iter("Quiz:*:*")]

            tasks = []
            keys_to_delete = [] # قائمة للمفاتيح التي يجب حذفها

            for key in all_quiz_keys_from_redis:
                # الهدف هو معالجة فقط المفاتيح الرئيسية لحالة المسابقة: Quiz:{bot_token}:{quiz_identifier}
                # المفاتيح الأخرى مثل QuizTime: و QuizAnswers: سيتم التعامل معها بواسطة وظائف أخرى
                # أو يجب أن يكون لها TTL (Time-To-Live) في Redis لتنتهي صلاحيتها تلقائياً.

                parts = key.split(':')

                # نركز فقط على المفاتيح التي تبدأ بـ "Quiz" وتتكون من 3 أجزاء بالضبط
                # هذا يستثني المفاتيح مثل Quiz:BOT:ID:Panel تلقائياً
                if len(parts) == 3 and parts[0] == 'Quiz' and parts[1] and parts[2]:
                    # هذا قد يكون مفتاح حالة مسابقة رئيسي. الآن نتحقق من محتواه.
                    quiz_status = await redis_handler.get_quiz_status_by_key(key)

                    # إذا كانت الحالة لا تحتوي على 'bot_token' أو 'quiz_identifier'
                    # أو إذا كان حقل 'status' غير موجود أو قيمته 'None'
                    # فهذا يشير إلى مفتاح رئيسي فاسد أو قديم
                    if not quiz_status or \
                            not quiz_status.get("bot_token") or \
                            not quiz_status.get("quiz_identifier") or \
                            quiz_status.get("status") is None: # تم التأكد من قيمة None هنا

                        logger.warning(f"Worker: Found potentially corrupted or incomplete main quiz key: {key}. Marking for deletion.")
                        keys_to_delete.append(key)
                    else:
                        # هذا مفتاح رئيسي سليم يبدو أنه يحتوي على البيانات الأساسية
                        # قم بإضافته إلى قائمة المهام ليتم معالجته بواسطة process_active_quiz
                        tasks.append(process_active_quiz(key))
                else:
                    # هذه المفاتيح ليست مفاتيح حالة مسابقة رئيسية (مثل QuizTime أو QuizAnswers
                    # أو مفاتيح Quiz:BOT:ID:Something التي لا يجب أن تكون hashes)
                    # يجب أن تُترك وشأنها ليتم التعامل معها من قبل RedisHandler أو TTL
                    # أو لكي تُحذف عند انتهاء المسابقة بشكل كامل.
                    logger.debug(f"Worker: Skipping non-main quiz key structure: {key}")

            # حذف المفاتيح الفاسدة أو القديمة التي تم تحديدها
            if keys_to_delete:
                logger.info(f"Worker: Deleting {len(keys_to_delete)} corrupted/old quiz keys: {keys_to_delete}")
                await redis_handler.redis_client.delete(*keys_to_delete)

            if tasks:
                logger.info(f"Worker: Found {len(tasks)} *valid main* quiz keys to process.")
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        # log the error with the key if possible, otherwise just the exception
                        # It's hard to map 'results' back to 'keys' directly if tasks are not ordered,
                        # but process_active_quiz already logs the key it's processing.
                        logger.error(f"Worker: An error occurred while processing a quiz task: {result}", exc_info=result)
            else:
                logger.debug("Worker: No valid main quiz keys found to process. Waiting...")

        except Exception as e:
            logger.error(f"Worker: A critical error occurred in the main loop: {e}", exc_info=True)

        await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Worker: Shutting down gracefully.")
    except Exception as e:
        logger.error(f"Worker: Fatal error in worker startup/main: {e}", exc_info=True)