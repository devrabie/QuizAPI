import aiosqlite
import asyncio
import json
import logging

logger = logging.getLogger(__name__)

async def create_tables(db_path: str):
    async with aiosqlite.connect(db_path) as db:
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
                answers TEXT, -- JSON string of answers
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
        await db.commit()

async def get_questions_general(db_path: str, count: int = 10) -> list:
    # جلب أسئلة عشوائية من كل الفئات (الخيار العام)
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM questions ORDER BY RANDOM() LIMIT ?', (count,))
        questions = await cursor.fetchall()
        return [dict(q) for q in questions]

# دالة جديدة: جلب أسئلة حسب اسم الفئة (التي تُرسل من PHP)
async def get_questions_by_category(db_path: str, category_name: str, count: int = 10) -> list:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM questions WHERE category = ? ORDER BY RANDOM() LIMIT ?', (category_name, count))
        questions = await cursor.fetchall()
        return [dict(q) for q in questions]

async def get_question_by_id(db_path: str, question_id: int) -> dict | None:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM questions WHERE id = ?', (question_id,))
        question = await cursor.fetchone()
        return dict(question) if question else None

async def save_quiz_history(db_path: str, quiz_identifier: str, total_questions: int, winner_id: int | None, winner_score: int | None) -> int:
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute(
            'INSERT INTO quiz_history (quiz_identifier, total_questions, winner_id, winner_score) VALUES (?, ?, ?, ?)',
            (quiz_identifier, total_questions, winner_id, winner_score)
        )
        await db.commit()
        return cursor.lastrowid

async def save_quiz_participant(db_path: str, quiz_history_id: int, user_id: int, score: int, answers: dict):
    async with aiosqlite.connect(db_path) as db:
        await db.execute(
            'INSERT INTO quiz_participants (quiz_history_id, user_id, score, answers) VALUES (?, ?, ?, ?)',
            (quiz_history_id, user_id, score, json.dumps(answers))
        )
        await db.commit()

async def update_user_stats(db_path: str, user_id: int, username: str, points: int, correct_answers: int, wrong_answers: int):
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
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM user_stats WHERE user_id = ?', (user_id,))
        stats = await cursor.fetchone()
        return dict(stats) if stats else None

async def get_leaderboard(db_path: str, limit: int = 10) -> list:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT user_id, username, total_points FROM user_stats ORDER BY total_points DESC LIMIT ?', (limit,))
        leaderboard = await cursor.fetchall()
        return [dict(row) for row in leaderboard]