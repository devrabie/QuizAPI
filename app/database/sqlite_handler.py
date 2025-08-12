import aiosqlite
import asyncio
import json
import logging

logger = logging.getLogger(__name__)

# --- نظام الترحيل (Migration System) ---

# الإصدار الحالي للمخطط. قم بزيادته عند إضافة ترحيل جديد.
CURRENT_DB_SCHEMA_VERSION = 3

async def _get_db_version(db: aiosqlite.Connection) -> int:
    """يسترد إصدار المخطط الحالي من قاعدة البيانات."""
    try:
        cursor = await db.execute("SELECT version FROM schema_info")
        row = await cursor.fetchone()
        return row[0] if row else 0
    except aiosqlite.OperationalError:
        return 0

async def _set_db_version(db: aiosqlite.Connection, version: int):
    """يضبط إصدار المخطط في قاعدة البيانات."""
    await db.execute('CREATE TABLE IF NOT EXISTS schema_info (version INTEGER PRIMARY KEY)')
    await db.execute("INSERT OR REPLACE INTO schema_info (version) VALUES (?)", (version,))
    logger.info(f"DB: تم تحديث إصدار مخطط الإحصائيات في القاعدة إلى {version}.")

async def _column_exists(db: aiosqlite.Connection, table_name: str, column_name: str) -> bool:
    """يتحقق مما إذا كان العمود موجودًا بالفعل في الجدول."""
    if not table_name.replace('_', '').isalnum():
        raise ValueError(f"اسم الجدول غير صالح: {table_name}")
    cursor = await db.execute(f"PRAGMA table_info({table_name})")
    columns = await cursor.fetchall()
    return any(col[1] == column_name for col in columns)

# --- دوال الترحيل الفردية ---

async def _migrate_to_v1(db: aiosqlite.Connection):
    """ترحيل إلى الإصدار 1: إنشاء الجداول الأولية (النقاط كـ INTEGER)."""
    await db.execute('''
        CREATE TABLE IF NOT EXISTS quiz_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quiz_identifier TEXT NOT NULL,
            total_questions INTEGER,
            winner_id INTEGER,
            winner_score INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    await db.execute('''
        CREATE TABLE IF NOT EXISTS quiz_participants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quiz_history_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            score INTEGER NOT NULL,
            answers TEXT,
            FOREIGN KEY (quiz_history_id) REFERENCES quiz_history(id)
        )
    ''')
    await db.execute('''
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            total_quizzes_played INTEGER DEFAULT 0,
            total_points INTEGER DEFAULT 0,
            correct_answers INTEGER DEFAULT 0,
            wrong_answers INTEGER DEFAULT 0,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    logger.info("DB: تم إنشاء الجداول الأولية (v1).")

async def _migrate_to_v2(db: aiosqlite.Connection):
    """ترحيل إلى الإصدار 2: إضافة عمود chat_id إلى quiz_history (بشكل آمن)."""
    if not await _column_exists(db, "quiz_history", "chat_id"):
        await db.execute('ALTER TABLE quiz_history ADD COLUMN chat_id INTEGER')
        logger.info("DB: تم إضافة عمود chat_id إلى quiz_history (v2).")
    else:
        logger.info("DB: العمود chat_id موجود بالفعل في quiz_history. تخطي الترحيل v2.")

async def _migrate_to_v3(db: aiosqlite.Connection):
    """
    ترحيل إلى الإصدار 3: تغيير أعمدة النقاط إلى REAL.
    يتطلب إعادة بناء الجداول.
    """
    await db.execute("ALTER TABLE quiz_history RENAME TO quiz_history_old")
    await db.execute('''
        CREATE TABLE quiz_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quiz_identifier TEXT NOT NULL,
            total_questions INTEGER,
            winner_id INTEGER,
            winner_score REAL,
            chat_id INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    await db.execute('''
        INSERT INTO quiz_history (id, quiz_identifier, total_questions, winner_id, winner_score, chat_id, timestamp)
        SELECT id, quiz_identifier, total_questions, winner_id, CAST(winner_score AS REAL), chat_id, timestamp
        FROM quiz_history_old
    ''')
    await db.execute("DROP TABLE quiz_history_old")
    logger.info("DB: تم ترحيل quiz_history إلى REAL (v3).")

    await db.execute("ALTER TABLE quiz_participants RENAME TO quiz_participants_old")
    await db.execute('''
        CREATE TABLE quiz_participants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quiz_history_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            score REAL NOT NULL,
            answers TEXT,
            FOREIGN KEY (quiz_history_id) REFERENCES quiz_history(id)
        )
    ''')
    await db.execute('''
        INSERT INTO quiz_participants (id, quiz_history_id, user_id, score, answers)
        SELECT id, quiz_history_id, user_id, CAST(score AS REAL), answers
        FROM quiz_participants_old
    ''')
    await db.execute("DROP TABLE quiz_participants_old")
    logger.info("DB: تم ترحيل quiz_participants إلى REAL (v3).")

    await db.execute("ALTER TABLE user_stats RENAME TO user_stats_old")
    await db.execute('''
        CREATE TABLE user_stats (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            total_quizzes_played INTEGER DEFAULT 0,
            total_points REAL DEFAULT 0.0,
            correct_answers INTEGER DEFAULT 0,
            wrong_answers INTEGER DEFAULT 0,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    await db.execute('''
        INSERT INTO user_stats (user_id, username, total_quizzes_played, total_points, correct_answers, wrong_answers, last_updated)
        SELECT user_id, username, total_quizzes_played, CAST(total_points AS REAL), correct_answers, wrong_answers, last_updated
        FROM user_stats_old
    ''')
    await db.execute("DROP TABLE user_stats_old")
    logger.info("DB: تم ترحيل user_stats إلى REAL (v3).")


async def ensure_db_schema_latest(db_path: str):
    """
    يضمن أن مخطط قاعدة بيانات الإحصائيات محدث.
    يطبق الترحيلات اللازمة بالتسلسل على الملف المحدد (stats_db_path).
    """
    db = None
    try:
        db = await aiosqlite.connect(db_path)

        current_version = await _get_db_version(db)
        logger.info(f"DB: {db_path} - إصدار مخطط الإحصائيات الحالي: {current_version}, الإصدار المطلوب: {CURRENT_DB_SCHEMA_VERSION}")

        if current_version < CURRENT_DB_SCHEMA_VERSION:
            logger.info(f"DB: {db_path} - بدء عملية ترحيل المخطط...")

            # تطبيق الترحيلات بالتسلسل، كل ترحيل في معاملة خاصة به
            if current_version < 1:
                async with db.transaction():
                    await _migrate_to_v1(db)
                    await _set_db_version(db, 1)
                current_version = 1

            if current_version < 2:
                async with db.transaction():
                    await _migrate_to_v2(db)
                    await _set_db_version(db, 2)
                current_version = 2

            if current_version < 3:
                # هذا الترحيل يتطلب تعطيل المفاتيح الخارجية
                await db.execute("PRAGMA foreign_keys = OFF;")
                async with db.transaction():
                    await _migrate_to_v3(db)
                    await _set_db_version(db, 3)
                await db.execute("PRAGMA foreign_keys = ON;")
                current_version = 3

            logger.info(f"DB: {db_path} - اكتملت عملية ترحيل المخطط بنجاح. الإصدار الجديد: {current_version}")
        else:
            logger.info(f"DB: {db_path} - مخطط الإحصائيات محدث بالفعل.")

    except Exception as e:
        logger.critical(f"DB: فشل حرج أثناء عملية ترحيل المخطط لـ {db_path}: {e}", exc_info=True)
        # لا حاجة لـ rollback هنا لأننا نستخدم `async with db.transaction()`
        raise
    finally:
        if db:
            # التأكد من إعادة تمكين المفاتيح الخارجية
            try:
                await db.execute("PRAGMA foreign_keys = ON;")
            except Exception as inner_e:
                logger.error(f"DB: فشل إعادة تمكين قيود المفاتيح الخارجية لـ {db_path}: {inner_e}")
            await db.close()

# --- دوال الوصول إلى قاعدة بيانات الأسئلة (تعمل على ملف منفصل) ---

async def get_questions_general(db_path: str, count: int = 10) -> list:
    """يستعلم من قاعدة بيانات الأسئلة (questions.db)."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM questions ORDER BY RANDOM() LIMIT ?', (count,))
        questions = await cursor.fetchall()
        return [dict(q) for q in questions]

async def get_questions_by_category(db_path: str, category_name: str, count: int = 10) -> list:
    """يستعلم من قاعدة بيانات الأسئلة (questions.db)."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM questions WHERE category = ? ORDER BY RANDOM() LIMIT ?', (category_name, count))
        questions = await cursor.fetchall()
        return [dict(q) for q in questions]

async def get_question_by_id(db_path: str, question_id: int) -> dict | None:
    """يستعلم من قاعدة بيانات الأسئلة (questions.db)."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM questions WHERE id = ?', (question_id,))
        question = await cursor.fetchone()
        return dict(question) if question else None


# --- دوال الوصول إلى قاعدة بيانات الإحصائيات (تعمل على ملف stats.db) ---

async def save_quiz_history(db_path: str, quiz_identifier: str, total_questions: int, winner_id: int | None, winner_score: float | None, chat_id: int | None) -> int:
    """يحفظ في قاعدة بيانات الإحصائيات (stats.db)."""
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            'INSERT INTO quiz_history (quiz_identifier, total_questions, winner_id, winner_score, chat_id) VALUES (?, ?, ?, ?, ?)',
            (quiz_identifier, total_questions, winner_id, winner_score, chat_id)
        )
        await db.commit()
        return cursor.lastrowid

async def save_quiz_participant(db_path: str, quiz_history_id: int, user_id: int, score: float, answers: dict):
    """يحفظ في قاعدة بيانات الإحصائيات (stats.db)."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            'INSERT INTO quiz_participants (quiz_history_id, user_id, score, answers) VALUES (?, ?, ?, ?)',
            (quiz_history_id, user_id, score, json.dumps(answers))
        )
        await db.commit()

async def update_user_stats(db_path: str, user_id: int, username: str, points: float, correct_answers: int, wrong_answers: int):
    """يحدث في قاعدة بيانات الإحصائيات (stats.db)."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute('''
            INSERT INTO user_stats (user_id, username, total_quizzes_played, total_points, correct_answers, wrong_answers, last_updated)
            VALUES (?, ?, 1, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                username = EXCLUDED.username,
                total_quizzes_played = total_quizzes_played + 1,
                total_points = total_points + EXCLUDED.total_points,
                correct_answers = correct_answers + EXCLUDED.correct_answers,
                wrong_answers = wrong_answers + EXCLUDED.wrong_answers,
                last_updated = CURRENT_TIMESTAMP
        ''', (user_id, username, points, correct_answers, wrong_answers))
        await db.commit()

async def get_user_stats(db_path: str, user_id: int) -> dict | None:
    """يستعلم من قاعدة بيانات الإحصائيات (stats.db)."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM user_stats WHERE user_id = ?', (user_id,))
        stats = await cursor.fetchone()
        return dict(stats) if stats else None

async def get_leaderboard(db_path: str, limit: int = 10) -> list:
    """يستعلم من قاعدة بيانات الإحصائيات (stats.db)."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT user_id, username, total_points FROM user_stats ORDER BY total_points DESC LIMIT ?', (limit,))
        leaderboard = await cursor.fetchall()
        return [dict(row) for row in leaderboard]

async def get_chat_leaderboard(db_path: str, chat_id: int, limit: int = 10) -> list:
    """يستعلم من قاعدة بيانات الإحصائيات (stats.db)."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT
                qp.user_id,
                us.username,
                SUM(qp.score) AS total_points_in_chat
            FROM quiz_participants qp
            JOIN quiz_history qh ON qp.quiz_history_id = qh.id
            LEFT JOIN user_stats us ON qp.user_id = us.user_id 
            WHERE qh.chat_id = ?
            GROUP BY qp.user_id, us.username
            ORDER BY total_points_in_chat DESC
            LIMIT ?
        ''', (chat_id, limit))
        leaderboard = await cursor.fetchall()
        return [{"user_id": row["user_id"], "username": row["username"], "score": round(row["total_points_in_chat"], 2)} for row in leaderboard]