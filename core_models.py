from pydantic import BaseModel, Extra, Field, field_validator, model_validator
from typing import Optional, Union, List, Set, Any
from datetime import datetime


class GroupBase(BaseModel):
    group_id: str
    name: str
    abbreviation: Optional[str] = None
    tags: Optional[str] = None


class RoarGroup(GroupBase):
    last_updated: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    current_activation_code: Optional[str] = None
    valid_activation_codes: Optional[list] = None


class LevanteGroup(GroupBase):
    created_at: Optional[datetime] = None


class DistrictBase(BaseModel):
    district_id: str
    name: str
    district_contact: Optional[dict] = None
    last_sync: Optional[datetime] = None
    launch_date: Optional[datetime] = None


class RoarDistrict(DistrictBase):
    abbreviation: Optional[str] = None
    clever: Optional[bool] = None
    current_activation_code: Optional[str] = None
    valid_activation_codes: Optional[list] = None
    portal_url: Optional[str] = None
    login_methods: Optional[list] = None
    mdr_number: Optional[str] = None
    pause_end_date: Optional[datetime] = None
    pause_start_date: Optional[datetime] = None
    schools: Optional[list] = None
    sis_type: Optional[str] = None
    last_updated: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    tags: Optional[list] = None


class SchoolBase(BaseModel):
    school_id: str
    district_id: str
    name: str
    school_number: Optional[str] = None
    state_id: Optional[str] = None
    high_grade: Optional[Union[int, str]] = None
    low_grade: Optional[Union[int, str]] = None
    location: Optional[dict] = None
    phone: Optional[str] = None
    principal: Optional[dict] = None
    created: Optional[datetime] = None
    last_modified: Optional[datetime] = None


class RoarSchool(SchoolBase):
    abbreviation: Optional[str] = None
    clever: Optional[bool] = None
    current_activation_code: Optional[str] = None
    valid_activation_codes: Optional[list] = None
    mdr_number: Optional[str] = None
    nces_id: Optional[str] = None
    last_updated: Optional[datetime] = None
    tags: Optional[list] = None


class ClassBase(BaseModel):
    class_id: str
    school_id: str
    district_id: str
    name: str
    subject: Optional[str] = None
    grade: Optional[Union[str, int]] = None
    created: Optional[datetime] = None
    last_modified: Optional[datetime] = None


class RoarClass(ClassBase):
    abbreviation: Optional[str] = None
    class_link: Optional[bool] = None
    class_link_app_id: Optional[str] = None
    current_activation_code: Optional[str] = None
    valid_activation_codes: Optional[list] = None
    grades: Optional[list] = None
    section_number: Optional[str] = None
    last_updated: Optional[datetime] = None
    tags: Optional[list] = None


class TaskBase(BaseModel):
    task_id: str
    name: str
    description: Optional[str] = None
    last_updated: datetime


class RoarTask(TaskBase):
    game_config: Optional[dict] = None
    image_url: Optional[str] = None
    tutorial_video_url: Optional[str] = None
    registered: Optional[bool] = None


class VariantBase(BaseModel):
    variant_id: str
    task_id: str
    name: Optional[str] = None
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


class LevanteVariant(VariantBase):
    pass


class RoarVariant(VariantBase):
    pass


class UserBase(BaseModel):
    user_id: str
    user_type: str
    assessment_pid: Optional[str] = None
    assessment_uid: Optional[str] = None
    email: Optional[str] = None
    email_verified: Optional[bool] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None


class RoarUser(UserBase):
    archived: Optional[bool] = None
    classes: Optional[dict] = None
    districts: Optional[dict] = None
    families: Optional[dict] = None
    grade: Optional[str] = None
    groups: Optional[dict] = None
    lab_id: Optional[str] = None
    name: Optional[dict] = None
    schools: Optional[dict] = None
    student_data: Optional[dict] = None
    legal: Optional[dict] = None
    school_level: Optional[str] = None
    user_type: Optional[str] = None
    username: Optional[str] = None


class LevanteUser(UserBase):
    parent_id: Optional[str] = None
    teacher_id: Optional[str] = None
    birth_year: Optional[int] = None  #Field(None, ge=1900, le=datetime.now().year)
    birth_month: Optional[int] = None  #Field(None, ge=1, le=12)
    sex: Optional[str] = None
    grade: Optional[Union[str, int]] = None

    _valid_group_ids: Set[str] = set()  # Private class attribute to hold valid group_ids

    @classmethod
    def set_valid_groups(cls, groups: List[LevanteGroup]):
        cls._valid_group_ids = {group.group_id for group in groups}

    # @model_validator(mode='before')
    # def check_user_in_valid_groups(cls, values):
    #     user_id = values.get('user_id', None)
    #     group_info = values.get('groups', {})
    #     current_group_ids = group_info.get('current', [])
    #     if current_group_ids:
    #         for group_id in current_group_ids:
    #             if group_id not in cls._valid_group_ids:
    #                 raise ValueError(f'{user_id} has current group_id {group_id} not in the list of valid groups.')
    #     else:
    #         raise ValueError(f'{user_id} do not have group information.')
    #     return values

    # @model_validator(mode='before')
    # def check_birth_year_for_students(cls, values):
    #     birth_year = values.get('birth_year', None)
    #     user_type = values.get('user_type', None)
    #     if birth_year and user_type == 'student':
    #         if birth_year < 2000:
    #             raise ValueError("Students must be born in or after 2000.")
    #     return values


class RunBase(BaseModel):
    run_id: str
    user_id: str
    task_id: str
    variant_id: str
    administration_id: Optional[str] = None
    reliable: Optional[bool] = None
    completed: Optional[bool] = None
    best_run: Optional[bool] = None
    task_version: Optional[str] = None
    time_started: Optional[datetime] = None
    time_finished: Optional[datetime] = None


class RoarRun(RunBase):
    scores: Optional[dict] = None
    user_data: Optional[dict] = None
    read_orgs: Optional[dict] = None
    tags: Optional[list] = None


class LevanteRun(RunBase):
    num_attempted: Optional[int] = None
    num_correct: Optional[int] = None
    test_comp_theta_estimate: Optional[float] = None
    test_comp_theta_se: Optional[float] = None


class TrialBase(BaseModel):
    # IDs
    trial_id: str
    user_id: str
    run_id: str
    task_id: str

    assessment_stage: str
    trial_index: Optional[int] = None
    item: Optional[str] = None
    answer: Optional[Union[int, str, float]] = None
    response: Optional[Union[int, str, float]] = None
    correct: Optional[bool] = None

    response_source: Optional[str] = None

    # Default jsPsych data attributes
    time_elapsed: Optional[int] = None

    # Time related fields
    rt: Optional[Union[int, str, dict]] = None
    server_timestamp: datetime


class RoarTrial(TrialBase):
    trial_type: Optional[str] = None
    # All other trial level data attributes
    trial_attributes: Optional[dict] = None


class LevanteTrial(TrialBase):
    is_practice_trial: Optional[bool] = None
    test_data: Optional[bool] = None
    corpus_trial_type: Optional[str] = None
    response_type: Optional[str] = None
    distractors: Optional[str] = None

    # For some roar tasks
    theta_estimate: Optional[float] = None
    theta_estimate2: Optional[float] = None
    theta_se: Optional[float] = None
    theta_se2: Optional[float] = None


class SurveyResponse(BaseModel):
    survey_response_id: str
    user_id: str
    class_friends: Optional[str] = None
    class_help: Optional[str] = None
    class_nice: Optional[str] = None
    class_play: Optional[str] = None
    example1_comic: Optional[str] = None
    example2_neat: Optional[str] = None
    growth_mind_math: Optional[str] = None
    growth_mind_read: Optional[str] = None
    growth_mind_smart: Optional[str] = None
    learning_good: Optional[str] = None
    lonely_school: Optional[str] = None
    math_enjoy: Optional[str] = None
    math_good: Optional[str] = None
    reading_enjoy: Optional[str] = None
    reading_good: Optional[str] = None
    school_enjoy: Optional[str] = None
    school_fun: Optional[str] = None
    school_give_up: Optional[str] = None
    school_happy: Optional[str] = None
    school_safe: Optional[str] = None
    teacher_like: Optional[str] = None
    teacher_listen: Optional[str] = None
    teacher_nice: Optional[str] = None
    created_at: datetime


class AdministrationBase(BaseModel):
    administration_id: str
    name: str
    public_name: Optional[str] = None
    sequential: bool
    created_by: str
    date_created: datetime
    date_closed: datetime
    date_opened: datetime


class RoarAdministration(AdministrationBase):
    assessments: Optional[list] = None
    districts: Optional[list] = None
    schools: Optional[list] = None
    classes: Optional[list] = None
    families: Optional[list] = None


class AdministrationTask(BaseModel):
    administration_id: str
    task_id: str
    variant_id: Optional[str] = None


class UserAssignment(BaseModel):
    user_id: str
    assignment_id: str
    started: bool
    date_time: datetime


class UserClass(BaseModel):
    user_id: str
    class_id: str
    is_active: bool


class UserGroup(BaseModel):
    user_id: str
    group_id: str
    is_active: bool
