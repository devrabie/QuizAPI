from pydantic import BaseModel
from typing import List, Optional

class StartCompetitionRequest(BaseModel):
    bot_token: str
    questions_db_path: str
    stats_db_path: str
    channel_id: str
    question_delay: int
    total_questions: int

class StopCompetitionRequest(BaseModel):
    bot_token: str
    channel_id: str

class SubmitAnswerRequest(BaseModel):
    bot_token: str
    user_id: int
    username: str
    question_id: int
    answer_index: int
    channel_id: str

class CompetitionStatusResponse(BaseModel):
    status: str
    current_question: Optional[int]
    total_questions: Optional[int]
    participants: int
    time_remaining: Optional[int]

class LeaderboardEntry(BaseModel):
    user_id: int
    username: str
    score: int

class LeaderboardResponse(BaseModel):
    leaderboard: List[LeaderboardEntry]
