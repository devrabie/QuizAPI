import aiosqlite
import json
from datetime import datetime

async def create_tables(db_path: str):
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS questions (
              id INTEGER PRIMARY KEY,
              owner_type TEXT NOT NULL,
              owner_id TEXT NOT NULL,
              category TEXT,
              question TEXT NOT NULL,
              opt1 TEXT, opt2 TEXT, opt3 TEXT, opt4 TEXT,
              correct_opt INTEGER
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_stats (
              user_id INTEGER,
              username TEXT,
              total_points INTEGER DEFAULT 0,
              total_answers INTEGER DEFAULT 0,
              correct_answers INTEGER DEFAULT 0,
              wrong_answers INTEGER DEFAULT 0,
              wins INTEGER DEFAULT 0,
              last_participation TIMESTAMP,
              PRIMARY KEY (user_id)
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS quiz_history (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              chat_id TEXT,
              started_at TIMESTAMP,
              ended_at TIMESTAMP,
              total_questions INTEGER,
              winner_id INTEGER,
              winner_score INTEGER
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS quiz_participants (
              quiz_id INTEGER,
              user_id INTEGER,
              score INTEGER,
              answers TEXT,
              FOREIGN KEY (quiz_id) REFERENCES quiz_history(id)
            );
        """)
        await db.commit()

async def get_questions(db_path: str, limit: int) -> list:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM questions ORDER BY RANDOM() LIMIT ?", (limit,))
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

async def get_question_by_id(db_path: str, question_id: int) -> dict:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM questions WHERE id = ?", (question_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None

async def update_user_stats(db_path: str, user_id: int, username: str, score: int, correct: bool):
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
            INSERT INTO user_stats (user_id, username, total_points, total_answers, correct_answers, wrong_answers, last_participation)
            VALUES (?, ?, ?, 1, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                total_points = total_points + excluded.total_points,
                total_answers = total_answers + 1,
                correct_answers = correct_answers + excluded.correct_answers,
                wrong_answers = wrong_answers + excluded.wrong_answers,
                last_participation = excluded.last_participation
        """, (user_id, username, score, 1 if correct else 0, 1 if not correct else 0, datetime.now()))
        await db.commit()

async def save_quiz_history(db_path: str, chat_id: str, total_questions: int, winner_id: int, winner_score: int):
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("""
            INSERT INTO quiz_history (chat_id, started_at, ended_at, total_questions, winner_id, winner_score)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (chat_id, datetime.now(), datetime.now(), total_questions, winner_id, winner_score))
        await db.commit()
        return cursor.lastrowid

async def save_quiz_participant(db_path: str, quiz_id: int, user_id: int, score: int, answers: dict):
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
            INSERT INTO quiz_participants (quiz_id, user_id, score, answers)
            VALUES (?, ?, ?, ?)
        """, (quiz_id, user_id, score, json.dumps(answers)))
        await db.commit()

async def get_leaderboard(db_path: str, limit: int = 10) -> list:
    async with aiosqlite.connect(db_path) as db:
        cursor = await db.execute("SELECT user_id, username, total_points FROM user_stats ORDER BY total_points DESC LIMIT ?", (limit,))
        rows = await cursor.fetchall()
        return [{"user_id": row[0], "username": row[1], "score": row[2]} for row in rows]
