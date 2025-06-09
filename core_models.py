from pydantic import BaseModel, Extra, Field, field_validator, model_validator, ValidationError
from typing import Optional, Union, List, Set, Any, Literal
from datetime import datetime
from zoneinfo import ZoneInfo
import ast


class GroupBase(BaseModel):
    group_id: str
    name: str
    abbreviation: Optional[str] = None
    tags: Optional[str] = None


class LevanteGroup(GroupBase):
    created_at: Optional[datetime] = None


class TaskBase(BaseModel):
    task_id: str
    name: str
    description: Optional[str] = None
    last_updated: datetime


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
    last_updated: Optional[datetime] = None


class TrialBase(BaseModel):
    # IDs
    trial_id: str
    user_id: str
    run_id: str
    task_id: str

    assessment_stage: str
    trial_index: Optional[Any] = None
    item: Optional[Any] = None
    item_id: Optional[str] = None
    item_uid: Optional[str] = None
    answer: Optional[Any] = None
    response: Optional[Any] = None
    correct: Optional[bool] = None
    difficulty: Optional[float] = None

    response_source: Optional[str] = None

    # Default jsPsych data attributes
    time_elapsed: Optional[int] = None

    # Time related fields
    rt: Optional[Any] = None
    server_timestamp: Optional[datetime] = None


class LevanteTrial(TrialBase):
    is_practice_trial: Optional[bool] = None
    corpus_trial_type: Optional[Any] = None
    response_type: Optional[str] = None
    response_location: Optional[Any] = None
    distractors: Optional[str] = None

    # For some roar tasks
    theta_estimate: Optional[float] = None  # Union[float, str]
    theta_estimate2: Optional[float] = None
    theta_se: Optional[float] = None
    theta_se2: Optional[float] = None

    valid_trial: Optional[bool] = None
    validation_msg_trial: Optional[list] = []
    warning_msg_trial: Optional[list] = []

    @model_validator(mode='after')
    def check_rt(self):
        rt_min = 100
        rt_max = 10000

        if self.assessment_stage not in ['instructions', 'practice_response']:
            if self.rt not in ["", "{}", "0", 0]:
                if isinstance(self.rt, int):
                    if self.task_id in ['matrix-reasoning']:
                        rt_min = 300
                        rt_max = 60000
                    elif self.task_id in ['egma-math']:
                        rt_max = 60000

                    if self.rt < rt_min:
                        self.validation_msg_trial.append(f"fast_rt_{rt_min / 1000}s")
                    elif self.rt > rt_max:
                        self.validation_msg_trial.append(f"slow_rt_{rt_max / 1000}s")
                elif isinstance(self.rt, str):
                    try:
                        rt_dict = ast.literal_eval(self.rt)
                        if not all([value > rt_min for value in rt_dict.values()]):
                            self.validation_msg_trial.append(f"fast_rt_{rt_min / 1000}s")
                        if not all([value < rt_max for value in rt_dict.values()]):
                            self.validation_msg_trial.append(f"slow_rt_{rt_max / 1000}s")
                    except Exception as e:
                        self.validation_msg_trial.append(f"rt string converted to dict failed as {e}")

            else:
                self.validation_msg_trial.append("rt_missing")
        return self

    @model_validator(mode='after')
    def check_trial_index(self):
        if self.trial_index:
            if not isinstance(self.trial_index, int):
                self.warning_msg_trial.append(f"trial_index_not_int")
        else:
            self.warning_msg_trial.append(f"trial_index_missing")
        return self

    @model_validator(mode='after')
    def update_valid_trial(self):
        self.valid_trial = True if not self.validation_msg_trial else False
        return self

    # @model_validator(mode='after')
    # def update_system_info(self):
    #     self.migration_datetime = datetime.now(ZoneInfo('America/Los_Angeles')).strftime('%Y-%m-%d')
    #     self.api_version = settings.config['VERSION']
    #     return self


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


class LevanteRun(RunBase):
    num_attempted: Optional[int] = None
    num_correct: Optional[int] = None
    test_comp_theta_estimate: Optional[float] = None
    test_comp_theta_se: Optional[float] = None

    valid_run: Optional[bool] = None
    validation_msg_run: Optional[list] = []
    warning_msg_run: Optional[list] = []

    _non_practice_trials: Optional[list[LevanteTrial]] = []

    def add_non_practice_trials(self, trial: LevanteTrial):
        self._non_practice_trials.append(trial)

    def check_non_practice_trials_count(self):
        trial_len_min = 10

        if len(self._non_practice_trials) < trial_len_min:
            self.validation_msg_run.append(f"less_than_{trial_len_min}_test_trials")

    def check_straight_line_trials(self):
        def sort_key(trial):
            index = trial.trial_index
            # Check if index is None or not an integer
            if index is None or not isinstance(index, int):
                # Handle None or non-integer by setting them to a high value or other logic
                return False, float('inf')  # Sorting None or invalid to the end
            return True, index  # Proper integers sorted normally

        def has_consecutive_identical(lst, n):
            # Check if the list has fewer than n elements or all elements are ""
            if len(lst) < n or all(x == "" for x in lst):
                return False

            # Loop through the list, stopping at the nth-to-last element
            for i in range(len(lst) - n + 1):
                # Check if the n elements starting from index i are all the same
                if all(lst[i] == lst[j] and lst[j] != "" for j in range(i, i + n)):
                    return True
            return False

        self._non_practice_trials.sort(key=sort_key)
        response_location = [trial.response_location for trial in self._non_practice_trials if
                             isinstance(trial.trial_index, int)]

        consecutive_identical_min = 10
        if has_consecutive_identical(response_location, consecutive_identical_min):
            self.validation_msg_run.append(f"straightlining_{consecutive_identical_min}")

    def update_valid_run(self):
        self.valid_run = True if not self.validation_msg_run else False
        return self

    # def update_system_info(self):
    #     self.migration_datetime = datetime.now(ZoneInfo('America/Los_Angeles')).strftime('%Y-%m-%d')
    #     self.api_version = settings.config['VERSION']
    #     return self

    def validate_trials_in_run(self):
        self.check_non_practice_trials_count()
        self.check_straight_line_trials()
        self.update_valid_run()


class UserBase(BaseModel):
    user_id: str
    user_type: str
    assessment_pid: Optional[str] = None
    email: Optional[str] = None
    email_verified: Optional[bool] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None


class LevanteUser(UserBase):
    parent1_id: Optional[str] = None
    parent2_id: Optional[str] = None
    teacher_id: Optional[str] = None
    birth_year: Optional[int] = None  #Field(None, ge=1900, le=datetime.now().year)
    birth_month: Optional[int] = None  # Field(None, ge=1, le=12)
    sex: Optional[str] = None
    grade: Optional[int] = None

    valid_user: Optional[bool] = None
    validation_msg_user: Optional[list] = []

    _valid_group_ids: Set[str] = set()  # Private class attribute to hold valid group_ids

    @classmethod
    def set_valid_groups(cls, groups: List[LevanteGroup]):
        cls._valid_group_ids = {group.group_id for group in groups}

    @model_validator(mode='after')
    def check_birth_year_month(self):
        if self.user_type == 'student':
            if self.birth_year and self.birth_month and isinstance(self.birth_year, int) and isinstance(
                    self.birth_month, int):
                if self.birth_month not in range(1, 13):
                    self.validation_msg_user.append("birth_month_error")
                if self.birth_year < 2000:
                    self.validation_msg_user.append("birth_year_under_2000")
                if self.birth_year > 2050:
                    self.validation_msg_user.append("birth_year_greater_2050")
            else:
                self.validation_msg_user.append("birth_year_month_missing")
        return self

    @model_validator(mode='after')
    def update_valid_user(self):
        self.valid_user = True if not self.validation_msg_user else False
        return self


class SurveyResponse(BaseModel):
    survey_response_id: str
    administration_id: Optional[str] = None
    user_id: str
    child_id: Optional[str] = None
    survey_id: str  # student, teacher, parent
    question_id: str  # TeacherGender, TeacherEducation

    boolean_response: Optional[bool] = None
    string_response: Optional[str] = None
    numeric_response: Optional[int] = None

    is_complete: Optional[bool] = None

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


class RoarGroup(GroupBase):
    last_updated: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    current_activation_code: Optional[str] = None
    valid_activation_codes: Optional[list] = None


class RoarTask(TaskBase):
    game_config: Optional[dict] = None
    image_url: Optional[str] = None
    tutorial_video_url: Optional[str] = None
    registered: Optional[bool] = None


class RoarTrial(TrialBase):
    trial_type: Optional[str] = None
    # All other trial level data attributes
    trial_attributes: Optional[dict] = None


class RoarRun(RunBase):
    scores: Optional[dict] = None
    user_data: Optional[dict] = None
    read_orgs: Optional[dict] = None
    tags: Optional[list] = None


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


class RoarAdministration(AdministrationBase):
    assessments: Optional[list] = None
    districts: Optional[list] = None
    schools: Optional[list] = None
    classes: Optional[list] = None
    families: Optional[list] = None
