from pydantic import BaseModel, Extra, Field, field_validator, ValidationError
from typing import Optional, Union
from datetime import datetime


class Group(BaseModel):
    group_id: str
    name: str
    abbreviation: Optional[str] = None
    last_updated: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    tags: Optional[list] = None


class District(BaseModel):
    district_id: str
    name: str
    abbreviation: Optional[str] = None
    clever: Optional[bool] = None
    portal_url: Optional[str] = None
    login_methods: Optional[list] = None
    district_contact: Optional[dict] = None
    mdr_number: Optional[str] = None
    pause_end_date: Optional[datetime] = None
    pause_start_date: Optional[datetime] = None
    schools: Optional[list] = None
    sis_type: Optional[str] = None
    last_sync: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    launch_date: Optional[datetime] = None
    tags: Optional[list] = None


class School(BaseModel):
    school_id: str
    district_id: str
    name: str
    abbreviation: Optional[str] = None
    clever: Optional[bool] = None
    school_number: Optional[str] = None
    state_id: Optional[str] = None
    high_grade: Optional[Union[int, str]] = None
    low_grade: Optional[Union[int, str]] = None
    mdr_number: Optional[str] = None
    nces_id: Optional[str] = None
    location: Optional[dict] = None
    phone: Optional[str] = None
    principal: Optional[dict] = None
    created: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    tags: Optional[list] = None


class Class(BaseModel):
    class_id: str
    school_id: str
    district_id: str
    name: str
    abbreviation: Optional[str] = None
    class_link: Optional[bool] = None
    class_link_app_id: Optional[str] = None
    subject: Optional[str] = None
    grade: Optional[Union[str, int]] = None
    grades: Optional[list] = None
    section_number: Optional[str] = None
    created: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    tags: Optional[list] = None


class User(BaseModel):
    user_id: str
    assessment_pid: Optional[str] = None
    user_type: str
    assessment_uid: Optional[str] = None
    parent_id: Optional[str] = None
    teacher_id: Optional[str] = None
    birth_year: Optional[int] = None
    birth_month: Optional[int] = None
    email: Optional[str] = None
    email_verified: Optional[bool] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None

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
    name: str
    description: Optional[str] = None
    game_config: Optional[dict] = None
    image_url: Optional[str] = None
    tutorial_video_url: Optional[str] = None
    registered: Optional[bool] = None
    last_updated: datetime

    class Config:
        extra = Extra.allow


class Variant(BaseModel):
    variant_id: str
    task_id: str
    variant_name: Optional[str] = None
    age: Optional[int] = None
    button_layout: Optional[str] = None
    corpus: Optional[str] = None
    key_helpers: Optional[bool] = None
    language: Optional[str] = None
    max_incorrect: Optional[int] = None
    max_time: Optional[int] = None
    num_of_practice_trials: Optional[int] = None
    num_of_trials: Optional[int] = None
    sequential_practice: Optional[bool] = None
    sequential_stimulus: Optional[bool] = None
    skip_instructions: Optional[bool] = None
    stimulus_blocks: Optional[int] = None
    store_item_id: Optional[bool] = None
    last_updated: datetime

    class Config:
        extra = Extra.allow

# class VariantParams(BaseModel):
#     variant_id: str
#     params_field: str
#     params_type: str
#     params_value: str


class Assignment(BaseModel):
    assignment_id: str
    name: str
    public_name: Optional[str] = None
    sequential: bool
    created_by: str
    date_created: datetime
    date_closed: datetime
    date_opened: datetime
    assessments: Optional[list] = None
    districts: Optional[list] = None
    schools: Optional[list] = None
    classes: Optional[list] = None
    groups: Optional[list] = None
    families: Optional[list] = None


class AssignmentTask(BaseModel):
    assignment_id: str
    task_id: str


class UserAssignment(BaseModel):
    assignment_id: str
    user_id: str
    started: bool
    completed: bool
    date_assigned: datetime
    date_closed: datetime
    date_opened: datetime
    assessments: Optional[list] = None
    progress: Optional[dict] = None
    assigning_orgs: Optional[dict] = None
    read_orgs: Optional[dict] = None
    minimal_orgs: Optional[dict] = None
    user_data: Optional[dict] = None


class Run(BaseModel):
    run_id: str
    user_id: str
    task_id: str
    variant_id: str
    assignment_id: Optional[str] = None
    reliable: Optional[bool] = None
    is_completed: Optional[bool] = None
    best_run: Optional[bool] = None
    scores: Optional[dict] = None
    time_started: Optional[datetime] = None
    time_finished: Optional[datetime] = None
    user_data: Optional[dict] = None
    read_orgs: Optional[dict] = None
    tags: Optional[list] = None


class Trial(BaseModel):
    # Related IDs
    trial_id: str
    user_id: str
    run_id: str
    task_id: str

    # Required Firekit attributes
    correct: int
    assessment_stage: str

    # Default jsPsych data attributes
    trial_index: Optional[int] = None
    trial_type: Optional[str] = None
    time_elapsed: Optional[int] = None

    # All other trial level data attributes
    trial_attributes: Optional[dict] = None

    # Answers related
    # item: Optional[str] = None
    # button_response: Optional[str] = None
    # response: Optional[Union[int, str, float]] = None
    # response_type: Optional[str] = None
    # response_source: Optional[str] = None
    # rt: Optional[Union[int, str]] = None

    # Trial attributes
    # is_practice: Optional[bool] = None
    # theta_estimate: Optional[float] = None
    # theta_se: Optional[float] = None
    # theta_estimate_2: Optional[float] = None
    # theta_se_2: Optional[float] = None
    # difficulty: Optional[float] = None
    # save_trial: Optional[bool] = None
    # server_timestamp: datetime
    #
    # class Config:
    #     extra = Extra.allow

    # @field_validator('rt')
    # def validate_rt(self, cls, v):
    #     if isinstance(v, int):
    #         if v <= 0:
    #             raise ValueError("Response time must be a positive integer")
    #     elif isinstance(v, str):
    #         # Only accept certain strings
    #         if v not in ["timeout", "unrecorded"]:
    #             raise ValueError("Invalid string for response time; allowed values are 'timeout' or 'unrecorded'")
    #     else:
    #         raise ValueError("Response time must be either an integer or one of the specific allowed strings")
    #     return v


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
