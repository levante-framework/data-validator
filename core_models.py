from pydantic import BaseModel, Field, model_validator
from typing import Optional, List
from datetime import datetime


class Class(BaseModel):
    class_id: str
    district_id: str
    school_id: str
    name: str
    section_number: Optional[str] = None
    subject: Optional[str] = None
    grade: Optional[str] = None
    created: Optional[datetime] = None
    last_modified: Optional[datetime] = None


class School(BaseModel):
    school_id: str
    district_id: str
    state_id: Optional[str] = None
    school_number: str
    name: str
    phone: Optional[str] = None
    high_grade: Optional[str] = None
    low_grade: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    principal_name: Optional[str] = None
    principal_email: Optional[str] = None
    created: Optional[datetime] = None
    lastModified: Optional[datetime] = None


class District(BaseModel):
    district_id: str
    name: str
    portal_url: str
    district_contact_name: Optional[str] = None
    district_contact_email: Optional[str] = None
    district_contact_title: Optional[str] = None
    last_sync: Optional[datetime] = None
    launch_date: Optional[datetime] = None


class Variant(BaseModel):
    variant_id: str
    task_id: str
    name: Optional[str] = None
    consent: Optional[bool] = None
    recruitment: Optional[str] = None
    skip_instructions: Optional[bool] = None
    story: Optional[bool] = None
    user_mode: Optional[str] = None
    last_updated: Optional[datetime] = None


class VariantParams(BaseModel):
    variant_id: str
    params_field: str
    params_type: str
    params_value: str


class Task(BaseModel):
    task_id: str
    name: str
    registered: bool = None
    description: str = None
    image: str = None
    last_updated: datetime


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


class Run(BaseModel):
    run_id: str
    user_id: str
    task_id: str
    variant_id: str
    assignment_id: str
    is_completed: bool
    score_composite: Optional[int] = None
    time_finished: Optional[datetime] = None
    time_started: Optional[datetime] = None


class Trial(BaseModel):
    trial_id: str
    user_id: str
    run_id: str
    task_id: str
    subtask_id: Optional[str] = None
    is_practice: bool
    internal_node_id: Optional[str] = None
    difficulty: Optional[str] = None
    trial_type: Optional[str] = None
    corpus_id: Optional[str] = None
    is_correct: bool
    trial_index: int
    response: str
    stimulus: str
    rt: int
    server_timestamp: datetime


class User(BaseModel):
    user_id: str
    assessment_pid: str
    assessment_uid: str
    user_type: str
    full_name: Optional[str] = None
    dob: Optional[datetime] = None
    gender: Optional[str] = None
    grade: Optional[str] = None
    state_id: Optional[str] = None
    races: Optional[str] = None
    hispanic_ethnicity: Optional[str] = None
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


class UserAssignment(BaseModel):
    user_id: str
    assignment_id: str
    is_started: bool
    date_time: datetime


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
