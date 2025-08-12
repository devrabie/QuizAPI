from pydantic import BaseModel, Field
from typing import Optional, List

class StartCompetitionRequest(BaseModel):
    bot_token: str
    quiz_identifier: str
    questions_db_path: str
    stats_db_path: str
    total_questions: int
    question_delay: int # هذا يمثل الآن "الوقت لكل سؤال"
    category: Optional[str] = None
    chat_id: Optional[int] = Field(None, description="معرف المحادثة التي تقام فيها المسابقة (للمجموعات/القنوات). يمكن أن يكون None للاستعلامات المضمنة.")

    # -- الإعدادات الجديدة --
    eliminate_after_x_wrong: Optional[int] = Field(None, description="عدد الإجابات الخاطئة التي تؤدي لإقصاء اللاعب. اتركه فارغاً أو 0 لتعطيله.")
    base_score_per_question: float = Field(1.0, description="النقاط الأساسية لكل إجابة صحيحة.")
    time_bonus_per_question: float = Field(0.5, description="أقصى نقاط إضافية يمكن كسبها من عامل السرعة (كسر عشري).")
    show_correct_answer_on_wrong: bool = Field(False, description="إظهار الإجابة الصحيحة للاعب إذا أجاب خطأ.")

class StopCompetitionRequest(BaseModel):
    bot_token: str
    quiz_identifier: str

class SubmitAnswerRequest(BaseModel):
    bot_token: str
    user_id: int
    username: str
    question_id: int
    answer_index: int
    quiz_identifier: str

class CompetitionStatusResponse(BaseModel):
    status: str
    participants: int
    current_question: Optional[int]
    total_questions: Optional[int]
    time_remaining: Optional[int]

class LeaderboardEntry(BaseModel):
    user_id: int
    username: str
    score: float # النقاط يمكن أن تكون عشرية

class LeaderboardResponse(BaseModel):
    leaderboard: List[LeaderboardEntry]