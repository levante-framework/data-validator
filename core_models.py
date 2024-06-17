from pydantic import BaseModel, Extra, Field, field_validator, model_validator
from typing import Optional, Union, List, Set
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
    birth_year: Optional[int] = None #Field(None, ge=1900, le=datetime.now().year)
    birth_month: Optional[int] = None #Field(None, ge=1, le=12)
    email: Optional[str] = None
    email_verified: Optional[bool] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None

    _valid_group_ids: Set[str] = set()  # Private class attribute to hold valid group_ids

    @classmethod
    def set_valid_groups(cls, groups: List[Group]):
        cls._valid_group_ids = {group.group_id for group in groups}

    @model_validator(mode='before')
    def check_user_in_valid_groups(cls, values):
        user_id = values.get('user_id', None)
        group_info = values.get('groups', {})
        current_group_ids = group_info.get('current', [])
        if current_group_ids:
            for group_id in current_group_ids:
                if group_id not in cls._valid_group_ids:
                    raise ValueError(f'{user_id} has current group_id {group_id} not in the list of valid groups.')
        else:
            raise ValueError(f'{user_id} do not have group information.')
        return values

    # @model_validator(mode='before')
    # def check_birth_year_for_students(cls, values):
    #     birth_year = values.get('birth_year', None)
    #     user_type = values.get('user_type', None)
    #     if birth_year and user_type == 'student':
    #         if birth_year < 2000:
    #             raise ValueError("Students must be born in or after 2000.")
    #     return values


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
    number_of_trials: Optional[int] = None
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
    task_version: Optional[str] = None
    time_started: Optional[datetime] = None
    # score_composite: Optional[int] = None
    time_finished: Optional[datetime] = None
    user_data: Optional[dict] = None
    read_orgs: Optional[dict] = None
    tags: Optional[list] = None


class Trial(BaseModel):
    # IDs
    trial_id: str
    user_id: str
    run_id: str
    task_id: str
    # sub_trial_id: Optional[int] = None

    # Answers related
    item: Optional[str] = None
    distract_options: Optional[str] = None
    expected_answer: Optional[Union[int, str, float]] = None
    response: Optional[Union[int, str, float]] = None
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
    trial_index: Optional[int] = None
    corpus_trial_type: Optional[str] = None
    assessment_stage: Optional[str] = None
    is_correct: Optional[bool] = None
    is_practice: Optional[bool] = None
    response_type: Optional[str] = None
    response_source: Optional[str] = None

    # Time related fields
    rt: Optional[Union[int, str]] = None
    time_elapsed: int
    server_timestamp: datetime

    # Roar tasks
    theta_estimate: Optional[float] = None
    theta_estimate2: Optional[float] = None
    theta_SE: Optional[float] = None
    theta_SE2: Optional[float] = None

    # @model_validator(mode='after')
    # def check_passwords_match(self):
    #     response = self.response
    #     expected_answer = self.expected_answer
    #     is_correct = self.is_correct
    #     if response is not None and expected_answer is not None and response != response:
    #         if is_correct is not None
    #         raise ValueError('passwords do not match')
    #     return self


class SurveyResponse(BaseModel):
    survey_response_id: str
    user_id: str
    class_friends: str
    class_help: str
    class_nice: str
    class_play: str
    example1_comic: str
    example2_neat: str
    growth_mind_math: str
    growth_mind_read: str
    growth_mind_smart: str
    learning_good: str
    lonely_school: str
    math_enjoy: str
    math_good: str
    reading_enjoy: str
    reading_good: str
    school_enjoy: str
    school_fun: str
    school_give_up: str
    school_happy: str
    school_safe: str
    teacher_like: str
    teacher_listen: str
    teacher_nice: str
    created_at: datetime

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
