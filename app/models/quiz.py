from pydantic import BaseModel
from typing import Optional

class StartCompetitionRequest(BaseModel):
    bot_token: str
    quiz_identifier: str  # Changed from channel_id
    questions_db_path: str
    stats_db_path: str
    total_questions: int
    question_delay: int
    category: Optional[str] = None # إضافة حقل الفئة هنا (سيكون اسم الفئة)



class StopCompetitionRequest(BaseModel):
    bot_token: str
    quiz_identifier: str # Changed from channel_id

class SubmitAnswerRequest(BaseModel):
    bot_token: str
    user_id: int
    username: str
    question_id: int
    answer_index: int
    quiz_identifier: str # Changed from channel_id

class CompetitionStatusResponse(BaseModel):
    status: str
    participants: int
    current_question: Optional[int]
    total_questions: Optional[int]
    time_remaining: Optional[int]

class LeaderboardEntry(BaseModel):
    user_id: int
    username: str
    score: int

class LeaderboardResponse(BaseModel):
    leaderboard: list[LeaderboardEntry]