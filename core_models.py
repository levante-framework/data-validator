from pydantic import BaseModel, Field, model_validator
from typing import Optional, List
from datetime import datetime


class Group(BaseModel):
    group_id: str
    name: str
    abbreviation: str


class District(BaseModel):
    district_id: str
    name: str
    portal_url: str
    district_contact_email: Optional[str] = None
    last_sync: Optional[datetime] = None
    launch_date: Optional[datetime] = None


class School(BaseModel):
    school_id: str
    district_id: str
    school_number: str
    name: str
    high_grade: Optional[int] = None
    low_grade: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    created: Optional[datetime] = None
    last_modified: Optional[datetime] = None


class Class(BaseModel):
    class_id: str
    school_id: str
    district_id: str
    name: str
    subject: Optional[str] = None
    grade: Optional[str] = None
    created: Optional[datetime] = None
    last_modified: Optional[datetime] = None


class User(BaseModel):
    user_id: str
    user_type: str
    assessment_uid: Optional[str] = None
    parent_id: Optional[str] = None
    teacher_id: Optional[str] = None
    birth_year: Optional[int] = None
    birth_month: Optional[int] = None
    email: Optional[str] = None
    email_verified: Optional[bool] = None
    created_at: Optional[datetime] = None
    last_updated: datetime

    # age: Optional[str] = None
    # assessment_pid: str
    # assessment_uid: str
    # full_name: Optional[str] = None
    # dob: Optional[datetime] = None
    # state_id: Optional[str] = None

    # @model_validator(mode='after')
    # def assign_attributes(self) -> 'User':
    #     if self.name.get("first", None) and self.name.get("last", None):
    #         self.fullName = f"{self.name.get("first", None)} {self.name.get("middle", None)}.{self.name.get("last", None)}"
    #     else:
    #         raise ValueError("Incomplete Name.")
    #     return self


class UserClass(BaseModel):
    user_id: str
    class_id: str
    is_active: bool


class UserGroup(BaseModel):
    user_id: str
    group_id: str
    is_active: bool


class Task(BaseModel):
    task_id: str
    description: str = None
    last_updated: datetime


class Variant(BaseModel):
    variant_id: str
    task_id: str
    name: Optional[str] = None
    last_updated: Optional[datetime] = None


class VariantParams(BaseModel):
    variant_params_id: str
    variant_id: str
    params_field: str
    params_type: str
    params_value: str


class Assignment(BaseModel):
    assignment_id: str
    name: str
    is_sequential: bool
    created_by: str
    date_created: datetime
    date_closed: datetime
    date_opened: datetime


class AssignmentTask(BaseModel):
    assignment_id: str
    task_id: str


class UserAssignment(BaseModel):
    assignment_id: str
    user_id: str
    is_started: bool
    is_completed: bool
    date_assigned: datetime


class Run(BaseModel):
    run_id: str
    user_id: str
    task_id: str
    variant_id: str
    assignment_id: Optional[str] = None
    is_reliable: Optional[bool] = None
    is_completed: Optional[bool] = None
    is_bestrun: Optional[bool] = None
    score_composite: Optional[int] = None
    time_started: Optional[datetime] = None
    time_finished: Optional[datetime] = None


class Trial(BaseModel):
    # Related IDs
    trial_id: str
    user_id: str
    run_id: str
    task_id: str

    # Answers related
    item: str
    distract_options: str
    expected_answer: str
    response: str
    response_type: str
    response_source: str
    is_correct: bool
    rt: int
    time_elapsed: int

    # Trial attributes
    trial_index: int
    is_practice: bool
    difficulty: Optional[float] = None
    trial_type: Optional[str] = None
    assessment_stage: Optional[str] = None
    server_timestamp: datetime


class Score(BaseModel):
    score_id: Optional[int] = None
    run_id: str
    is_computed: Optional[bool] = False
    is_composite: Optional[bool] = False
    is_practice: Optional[bool] = False
    subtask_name: Optional[str] = None
    score_type: Optional[str] = None
    score: Optional[str]
    attempted_note: Optional[str] = None
    correct_note: Optional[str] = None
    incorrect_note: Optional[str] = None
    theta_estimate: Optional[float] = None
    theta_se: Optional[float] = None
