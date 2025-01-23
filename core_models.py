from pydantic import BaseModel, Extra, Field, field_validator, model_validator, ValidationError
from typing import Optional, Union, List, Set, Any, Literal
from datetime import datetime
from zoneinfo import ZoneInfo
import ast

import settings


class GroupBase(BaseModel):
    group_id: str
    name: str
    abbreviation: Optional[str] = ""
    tags: Optional[str] = ""


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
    last_updated: Optional[datetime] = None


class LevanteVariant(VariantBase):
    pass


class RoarVariant(VariantBase):
    pass


class TrialBase(BaseModel):
    # IDs
    trial_id: str
    user_id: str
    run_id: str
    task_id: str

    assessment_stage: str
    trial_index: Optional[Any] = None
    item: Optional[Any] = None
    item_id: Optional[str] = ""
    answer: Optional[Any] = None
    response: Optional[Any] = None
    correct: Optional[bool] = None
    difficulty: Optional[float] = None

    response_source: Optional[str] = ""

    # Default jsPsych data attributes
    time_elapsed: Optional[int] = None

    # Time related fields
    rt: Optional[Union[int, str]] = ""
    server_timestamp: datetime


class RoarTrial(TrialBase):
    trial_type: Optional[str] = None
    # All other trial level data attributes
    trial_attributes: Optional[dict] = None


class LevanteTrial(TrialBase):
    is_practice_trial: Optional[bool] = None
    corpus_trial_type: Optional[Union[str, int]] = ""
    response_type: Optional[str] = ""
    response_location: Optional[Union[int, str]] = ""
    distractors: Optional[str] = ""

    # For some roar tasks
    theta_estimate: Optional[float] = None  # Union[float, str]
    theta_estimate2: Optional[float] = None
    theta_se: Optional[float] = None
    theta_se2: Optional[float] = None

    pass_validation: Optional[bool] = None
    validation_err_msg: Optional[list] = []

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
                        self.validation_err_msg.append(f"fast_rt_{rt_min / 1000}s")
                    elif self.rt > rt_max:
                        self.validation_err_msg.append(f"slow_rt_{rt_max / 1000}s")
                elif isinstance(self.rt, str):
                    try:
                        rt_dict = ast.literal_eval(self.rt)
                        if not all([value > rt_min for value in rt_dict.values()]):
                            self.validation_err_msg.append(f"fast_rt_{rt_min / 1000}s")
                        if not all([value < rt_max for value in rt_dict.values()]):
                            self.validation_err_msg.append(f"slow_rt_{rt_max / 1000}s")
                    except Exception as e:
                        self.validation_err_msg.append(f"rt string converted to dict failed as {e}")

            else:
                self.validation_err_msg.append("rt_missing")
        return self

    @model_validator(mode='after')
    def check_trial_index(self):
        if self.trial_index:
            if not isinstance(self.trial_index, int):
                self.validation_err_msg.append(f"trial_index_not_int")
        else:
            self.validation_err_msg.append(f"trial_index_missing")
        return self

    @model_validator(mode='after')
    def update_pass_validation(self):
        self.pass_validation = True if not self.validation_err_msg else False
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

    pass_validation: Optional[bool] = None
    validation_err_msg: Optional[list] = []
    warning_msg: Optional[list] = []

    _trials: Optional[list[LevanteTrial]] = []

    def add_levante_trial(self, trial: LevanteTrial):
        self._trials.append(trial)

    def check_trials_count(self):
        trial_len_min = 3

        if len(self._trials) < trial_len_min:
            self.warning_msg.append(f"less_than_{trial_len_min}_trials")

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

        self._trials.sort(key=sort_key)
        response_location = [trial.response_location for trial in self._trials if isinstance(trial.trial_index, int)]

        consecutive_identical_min = 10
        if has_consecutive_identical(response_location, consecutive_identical_min):
            self.validation_err_msg.append(f"straightlining_{consecutive_identical_min}")

    def update_pass_validation(self):
        self.pass_validation = True if not self.validation_err_msg else False
        return self

    # def update_system_info(self):
    #     self.migration_datetime = datetime.now(ZoneInfo('America/Los_Angeles')).strftime('%Y-%m-%d')
    #     self.api_version = settings.config['VERSION']
    #     return self

    def validate_trials_in_run(self):
        self.check_trials_count()
        self.check_straight_line_trials()
        self.update_pass_validation()


class UserBase(BaseModel):
    user_id: str
    user_type: str
    assessment_pid: Optional[str] = ""
    assessment_uid: Optional[str] = ""
    email: Optional[str] = ""
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
    parent1_id: Optional[str] = ""
    parent2_id: Optional[str] = ""
    teacher_id: Optional[str] = ""
    birth_year: Optional[int] = None  #Field(None, ge=1900, le=datetime.now().year)
    birth_month: Optional[int] = None  # Field(None, ge=1, le=12)
    sex: Optional[str] = ""
    grade: Optional[Union[str, int]] = ""

    pass_validation: Optional[bool] = None
    validation_err_msg: Optional[list] = []

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

    @model_validator(mode='after')
    def check_birth_year_month(self):
        if self.user_type == 'student':
            if self.birth_year and self.birth_month and isinstance(self.birth_year, int) and isinstance(
                    self.birth_month, int):
                if self.birth_month not in range(1, 13):
                    self.validation_err_msg.append("birth_month_error")
                if self.birth_year < 2000:
                    self.validation_err_msg.append("birth_year_2000")
            else:
                self.validation_err_msg.append("birth_year_month_missing")
        return self

    @model_validator(mode='after')
    def update_pass_validation(self):
        self.pass_validation = True if not self.validation_err_msg else False
        return self

    # @model_validator(mode='after')
    # def update_system_info(self):
    #     self.migration_datetime = datetime.now(ZoneInfo('America/Los_Angeles')).strftime('%Y-%m-%d')
    #     self.api_version = settings.config['VERSION']
    #     return self


class SurveyQuestion(BaseModel):
    survey_question_id: str
    survey_question: str
    survey_type: str


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


# class StudentSurveyResponse(SurveyResponse):
#     Example1Comic: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     Example2Neat: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherNice: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherLike: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherListen: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     SchoolFun: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     SchoolEnjoy: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     SchoolHappy: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     SchoolSafe: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ClassFriends: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ClassHelp: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ClassPlay: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ClassNice: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     LonelySchool: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     LearningGood: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     SchoolGiveUp: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ReadingEnjoy: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     MathEnjoy: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ReadingGood: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     MathGood: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     GrowthMindSmart: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     GrowthMindRead: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     GrowthMindMath: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#
#
# class TeacherSurveyResponse(SurveyResponse):
#     TeacherAge: Optional[int] = Field(None, description="Accepts only 10-100 as valid inputs", ge=10, le=100)
#     TeacherGender: Optional[str] = ""
#     TeacherGenderComment: Optional[str] = ""
#     TeacherEducation: Optional[str] = ""
#     TeacherUndergrad: Optional[str] = ""
#     TeacherUndergradOtherEd: Optional[str] = ""
#     TeacherUndergradNonEd: Optional[str] = ""
#     TeacherUndergradOther: Optional[str] = ""
#     TeacherGrad: Optional[str] = ""
#     TeacherGradOtherEd: Optional[str] = ""
#     TeacherGradNonEd: Optional[str] = ""
#     TeacherGradOther: Optional[str] = ""
#     TeacherYears: Optional[str] = ""
#     TeacherYearsSchool: Optional[str] = ""
#     TeacherClimate1: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherClimate2: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherClimate3: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherClimate4: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherClimate5: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherClimate6: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherClimate7a: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherClimate7b: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherClimate8: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherClimate9: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherClimate10: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#
#     TeacherFeelJob1: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
#     TeacherFeelJob2: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
#     TeacherFeelJob3: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
#     TeacherFeelJob5: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
#     TeacherFeelJob6: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
#     TeacherFeelJob7: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
#     TeacherFeelJob9: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
#     TeacherFeelJob10: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
#     TeacherFeelJob12: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
#     TeacherFeelJob13: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
#     TeacherFeelJob14: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
#     TeacherFeelJob15: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
#     TeacherFeelJob16: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
#
#     TeacherBeliefTeach1: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     TeacherBeliefTeach2: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     TeacherBeliefTeach3: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     TeacherBeliefTeach4: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     TeacherBeliefTeach6: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     TeacherBeliefTeach7: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     TeacherBeliefTeach8: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     TeacherBeliefTeach9: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     TeacherBeliefTeach10: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     TeacherBeliefTeach11: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     TeacherBeliefTeach12: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     TeacherBeliefTeach5: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#
#     TeacherIdeasChildren1: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren2: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren3: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren4: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren5: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren6: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren7: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren8: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren9: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren10: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren11: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren12: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren13: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren14: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren15: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherIdeasChildren16: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#
#     TeacherNumberClasses: Optional[int] = Field(None, description="Accepts only 1-7 as valid inputs", ge=1, le=7)
#     TeacherOtherAdult: Optional[str] = ""
#     TeacherStudentsEnroll: Optional[int] = None
#     TeacherStudentsGender: Optional[str] = ""
#     TeacherStudentsGenderGirls: Optional[int] = None
#     TeacherStudentsGenderBoys: Optional[int] = None
#     TeacherStudentsGenderNonbinary: Optional[int] = None
#     TeacherStudentsGenderOther: Optional[int] = None
#     TeacherStudentsAges: Optional[str] = ""
#     TeacherStudentsAges5: Optional[int] = None
#     TeacherStudentsAges6: Optional[int] = None
#     TeacherStudentsAges7: Optional[int] = None
#     TeacherStudentsAges8: Optional[int] = None
#     TeacherStudentsAges9: Optional[int] = None
#     TeacherStudentsAges10: Optional[int] = None
#     TeacherStudentsAges11: Optional[int] = None
#     TeacherStudentsAges12: Optional[int] = None
#     TeacherStudentsAges13: Optional[int] = None
#     TeacherMultigrade: Optional[str] = ""
#     TeacherMultigradeLevels: Optional[str] = ""
#     TeacherMultigradeLevelsComment: Optional[str] = ""
#     TeacherGradeLevel: Optional[str] = ""
#     TeacherGradeLevelComment: Optional[str] = ""
#     TeacherLanguageStudents: Optional[str] = ""
#     TeacherLanguageStudentsComment: Optional[str] = ""
#     TeacherLanguageTeachers: Optional[str] = ""
#     TeacherLanguageTeachersComment: Optional[str] = ""
#     TeacherReadingBelow: Optional[int] = None
#     TeacherReadingAbove: Optional[int] = None
#     TeacherMathBelow: Optional[int] = None
#     TeacherMathAbove: Optional[int] = None
#     TeacherReadingFrequency: Optional[str] = ""
#     TeacherReadingTime: Optional[str] = ""
#     TeacherMathFrequency: Optional[str] = ""
#     TeacherMathTime: Optional[str] = ""
#     TeacherFamilySupport: Optional[str] = ""
#     TeacherFamilyOverwhelm: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherFamilyHelping: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     TeacherComm: Optional[str] = ""
#     TeacherCommFamily: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     TeacherCommStudents: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#
#
# class CaregiverSurveyResponse(SurveyResponse):
#     RespondentRelationship: Optional[str] = ""
#     RespondentRelationshipComment: Optional[str] = ""
#     RespondentRelationshipOtherSpecify: Optional[str] = ""
#     RespondentTimeCaring: Optional[str] = ""
#     RespondentTimeCaringYears: Optional[str] = ""
#     RespondentTimeCaringMonths: Optional[str] = ""
#     ChildAgeYears: Optional[int] = Field(None, description="Accepts only 0-15 as valid inputs", ge=0, le=15)
#     ChildHeightCurrent: Optional[str] = ""
#     ChildHeightCurrentFeet: Optional[str] = ""
#     ChildHeightCurrentInches: Optional[str] = ""
#     ChildWeightCurrent: Optional[str] = ""
#     ChildWeightCurrentPounds: Optional[str] = ""
#     ChildBornEarly: Optional[str] = ""
#     ChildWeightBirth: Optional[str] = ""
#     ChildWeightBirthPounds: Optional[str] = ""
#     ChildWeightBirthOunces: Optional[str] = ""
#     ChildHealth: Optional[int] = Field(None, description="Accepts only 1-5 as valid inputs", ge=1, le=5)
#     ChildTeeth: Optional[int] = Field(None, description="Accepts only 1-5 as valid inputs", ge=1, le=5)
#     ChildPhysicalActivity: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildChronicHealth: Optional[str] = ""
#     ChildBreathing: Optional[str] = ""
#     ChildEating: Optional[str] = ""
#     ChildDigestion: Optional[str] = ""
#     ChildPain: Optional[str] = ""
#     ChildToothache: Optional[str] = ""
#     ChildGums: Optional[str] = ""
#     ChildCavities: Optional[str] = ""
#     ChildHealthOther: Optional[str] = ""
#     ChildConcentration: Optional[str] = ""
#     ChildStairs: Optional[str] = ""
#     ChildDressBathe: Optional[str] = ""
#     ChildHearing: Optional[str] = ""
#     ChildEyesight: Optional[str] = ""
#     ChildVisionHearingOther: Optional[str] = ""
#     ChildVisionHearingExp: Optional[str] = ""
#     ChildGenetic: Optional[str] = ""
#     ChildGeneticExp: Optional[str] = ""
#     ChildHealthConditions: Optional[str] = ""
#     ChildConduct: Optional[str] = ""
#     ChildConductCurrent: Optional[str] = ""
#     ChildConductLevel: Optional[int] = Field(None, description="Accepts only 1-3 as valid inputs", ge=1, le=3)
#     ChildDevDelay: Optional[str] = ""
#     ChildDevDelayCurrent: Optional[str] = ""
#     ChildDevDelayLevel: Optional[int] = Field(None, description="Accepts only 1-3 as valid inputs", ge=1, le=3)
#     ChildIntellDis: Optional[str] = ""
#     ChildIntellDisCurrent: Optional[str] = ""
#     ChildIntellDisLevel: Optional[int] = Field(None, description="Accepts only 1-3 as valid inputs", ge=1, le=3)
#     ChildSpeechLang: Optional[str] = ""
#     ChildSpeechLangCurrent: Optional[str] = ""
#     ChildSpeechLangLevel: Optional[int] = Field(None, description="Accepts only 1-3 as valid inputs", ge=1, le=3)
#     ChildLearnDis: Optional[str] = ""
#     ChildLearnDisCurrent: Optional[str] = ""
#     ChildLearnDisLevel: Optional[int] = Field(None, description="Accepts only 1-3 as valid inputs", ge=1, le=3)
#     ChildAutism: Optional[str] = ""
#     ChildAutismCurrent: Optional[str] = ""
#     ChildAutismLevel: Optional[int] = Field(None, description="Accepts only 1-3 as valid inputs", ge=1, le=3)
#     ChildADHD: Optional[str] = ""
#     ChildADHDCurrent: Optional[str] = ""
#     ChildADHDLevel: Optional[int] = Field(None, description="Accepts only 1-3 as valid inputs", ge=1, le=3)
#     ChildSleep: Optional[str] = ""
#     ChildSleepHabits: Optional[str] = ""
#     ChildSleep1: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildSleep2: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildSleep5: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildSleepHours: Optional[int] = Field(None, description="Accepts only 1-7 as valid inputs", ge=1, le=7)
#     ChildSexBirth: Optional[Literal["Female", "Male"]] = Field(None,
#                                                                description="Accepts only Male and Female as valid inputs")
#     ChildGenderCurrent: Optional[Literal["Female", "Male", "Nonbinary", "Other"]] = Field(None,
#                                                                                           description="Accepts only Female,Male,Nonbinary,Other as valid inputs")
#     ChildGenderCurrentComment: Optional[str] = ""
#     ChildBehGirls: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     ChildBeGirl: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     ChildBehBoys: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     ChildBeBoy: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     ChildPubertyYN: Optional[str] = ""
#     ChildPubertyFemale: Optional[str] = ""
#     ChildPubertyGrowthFemale: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildPubertyHairFemale: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildPubertySkinFemale: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildPubertyBreastFemale: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildPubertyMenstruate: Optional[str] = ""
#     ChildPubertyMenstruateAge: Optional[str] = ""
#     ChildPubertyMale: Optional[str] = ""
#     ChildPubertyGrowthMale: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildPubertyHairMale: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildPubertySkinMale: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildPubertyVoiceMale: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildPubertyFaceHairMale: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQitems: Optional[str] = ""
#     ChildSDQ2: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQ3: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQ6: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQ8: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQ10: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQ11: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQ13: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQ14: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQ15: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQ16: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQ17: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQ18: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQ22: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQ23: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSDQ24: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSCSitems: Optional[str] = ""
#     ChildSCS1: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSCS2: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSCS3: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSCS4: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSCS5: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSCS6: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSCS7: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSCS8: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSCS9: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSCS10: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSCS11: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildSCS12: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildCBQitems: Optional[str] = ""
#     ChildCBQ1: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildCBQ2: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildCBQ3: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildCBQ4: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildCBQ5: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildCBQ6: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildCBQ7: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildCBQ9: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildCBQ10: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildCBQ11: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukesItems: Optional[str] = ""
#     ChildJukes1: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes2: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes3: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes4: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes5: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes6: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes7: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes8: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes9: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes10: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes11: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes12: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes13: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes14: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes15: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes16: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes17: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes18: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes19: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildJukes20: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildFriends: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildBullied: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     ChildBullyOthers: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     ChildELSitems: Optional[str] = ""
#     ChildELS1: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS2: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS3: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS4: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS5: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS6: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS7: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS8: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS9: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS10: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS11: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS12: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS13: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS14: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS15: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS16: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS17: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildELS18: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     ChildAttendEC: Optional[str] = ""
#     ChildAttendKinder: Optional[str] = ""
#     ChildAttendECKinderTime: Optional[int] = Field(None, description="Accepts only 0-5 as valid inputs", ge=0, le=5)
#     ChildAttendPrimaryAge: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEdAchieve: Optional[str] = ""
#     ChildSchoolItems: Optional[str] = ""
#     ChildNSCHg10a: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildNSCHg10b: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildNSCHg10c: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildNSCHg10d: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildNSCHg10e: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     ChildDeviceUse: Optional[int] = Field(None, description="Accepts only 1-5 as valid inputs", ge=1, le=5)
#     ChildPhone: Optional[str] = ""
#     ChildPhoneAge: Optional[str] = ""
#     ChildSocialMedia: Optional[str] = ""
#     ChildEFQ: Optional[str] = ""
#     ChildEFQ1: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ2: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ3: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ4: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ5: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ6: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ7: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ8: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ9: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ10: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ11: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ12: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ13: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ14: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ15: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ16: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ17: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ18: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ19: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ20: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ21: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ22: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ23: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ24: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ25: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ26: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ27: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ28: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ29: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ30: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ31: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ32: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ33: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     ChildEFQ34: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#
#     HomeHOME1: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     HomeHOME2: Optional[int] = Field(None, description="Accepts only 0-5 as valid inputs", ge=0, le=5)
#     HomeHOME3: Optional[str] = ""
#     HomeHOME4: Optional[str] = ""
#     HomeHOME5: Optional[int] = Field(None, description="Accepts only 1-5 as valid inputs", ge=1, le=5)
#     HomeHOME6: Optional[int] = Field(None, description="Accepts only 0-5 as valid inputs", ge=0, le=5)
#     HomeHOME7: Optional[int] = Field(None, description="Accepts only 0-5 as valid inputs", ge=0, le=5)
#     HomeHOME9: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     HomeHOME15: Optional[str] = ""
#     HomeHOME23: Optional[str] = ""
#     HomeHOME22: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     HomeHOME24: Optional[str] = ""
#     HomeHOME24a: Optional[str] = ""
#     HomeHOME24b: Optional[str] = ""
#     HomeHOME24c: Optional[str] = ""
#     HomeHOME24d: Optional[str] = ""
#     HomeHOME24e: Optional[str] = ""
#     HomeHOME24f: Optional[str] = ""
#     HomeHOME24g: Optional[str] = ""
#     HomeHOME24h: Optional[str] = ""
#     HomeHOME24i: Optional[str] = ""
#     HomeHOME24j: Optional[str] = ""
#     HomeHOME25: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     HomeHOME26: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     HomeHOME27: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     HomeHOME28: Optional[str] = ""
#     HomeHOME29: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     HomeHOME30: Optional[str] = ""
#     HomeHOME31: Optional[str] = ""
#     HomeHOME32: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     HomeHOME33: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     HomeHOME34: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     HomeHOME35: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     HomeHOME36: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     HomeHOME37: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     HomeHOME38: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     SelfParentStressNCSHh9: Optional[str] = ""
#     SelfParentStressNSCHh9a: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     SelfParentStressNSCHh9b: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     SelfParentStressNSCHh9c: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
#     SelfParentRohner: Optional[str] = ""
#     SelfParentRohnerC3: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     SelfParentRohnerC7: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     SelfParentRohnerC14: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     SelfParentRohnerW15: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     SelfParentRohnerC20: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     SelfParentRohnerW21: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     SelfParentRohnerW23: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     SelfParentRohnerC26: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     SelfParentRohnerW27: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     SelfParentRohnerW29: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     SelfParentMICS: Optional[str] = ""
#     SelfParentMICS1: Optional[str] = ""
#     SelfParentMICS2: Optional[str] = ""
#     SelfParentMICS4: Optional[str] = ""
#     SelfParentMICS6: Optional[str] = ""
#     SelfParentMICS12: Optional[str] = ""
#     HomeNeighborhood: Optional[str] = ""
#     HomeNSCHi7a: Optional[str] = ""
#     HomeNSCHi7b: Optional[str] = ""
#     HomeNSCHi7c: Optional[str] = ""
#     HomeNSCHi7d: Optional[str] = ""
#     HomeNSCHi7e: Optional[str] = ""
#     HomeNSCHi7f: Optional[str] = ""
#     HomeNSCHi7g: Optional[str] = ""
#     HomeNSCHi8items: Optional[str] = ""
#     HomeNSCHi8a: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     HomeNSCHi8b: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     HomeNSCHi8c: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     HomeNSCHi8d: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     HomeNSCHi8e: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     HomeFood: Optional[str] = ""
#     HomeFoodLast: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     HomeFoodBalanced: Optional[int] = Field(None, description="Accepts only 0-2 as valid inputs", ge=0, le=2)
#     HomeFoodSkipMeal: Optional[str] = ""
#     HomeFoodSkipMealAmount: Optional[int] = Field(None, description="Accepts only 1-3 as valid inputs", ge=1, le=3)
#     HomeCHAOS: Optional[str] = ""
#     HomeCHAOS4: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     HomeCHAOS10: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     HomeCHAOS14: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     HomeCHAOS15: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     SelfParentStressNSCHh8: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=4)
#     SelfAnxDep: Optional[str] = ""
#     SelfAnxDep1: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     SelfAnxDep2: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     SelfAnxDep3: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     SelfAnxDep4: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
#     SelfSupport: Optional[str] = ""
#     SelfSupport1: Optional[int] = Field(None, description="Accepts only 1-7 as valid inputs", ge=1, le=7)
#     SelfSupport2: Optional[int] = Field(None, description="Accepts only 1-7 as valid inputs", ge=1, le=7)
#     SelfSupport3: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=7)
#     SelfSupport4: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=7)
#     SelfSupport5: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=7)
#     SelfSupport6: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=7)
#     SelfSupport7: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=7)
#     SelfSupport8: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=7)
#     SelfSupport9: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=7)
#     SelfSupport10: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=7)
#     SelfSupport11: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=7)
#     SelfSupport12: Optional[int] = Field(None, description="Accepts only 1-4 as valid inputs", ge=1, le=7)
#     SelfDiscrim: Optional[str] = ""
#     SelfDiscrim1: Optional[int] = Field(None, description="Accepts only 0-5 as valid inputs", ge=0, le=5)
#     SelfDiscrim2: Optional[int] = Field(None, description="Accepts only 0-5 as valid inputs", ge=0, le=5)
#     SelfDiscrim3: Optional[int] = Field(None, description="Accepts only 0-5 as valid inputs", ge=0, le=5)
#     SelfDiscrim4: Optional[int] = Field(None, description="Accepts only 0-5 as valid inputs", ge=0, le=5)
#     SelfDiscrim5: Optional[int] = Field(None, description="Accepts only 0-5 as valid inputs", ge=0, le=5)
#     SelfLifeChanges: Optional[str] = ""
#     SelfLifeChangesHealth: Optional[str] = ""
#     SelfLifeChanges1: Optional[Literal[0, 74]] = Field(None, description="Allows only 0 or 74")
#     SelfLifeChanges2: Optional[Literal[0, 44]] = Field(None, description="Allows only 0 or 44")
#     SelfLifeChanges3: Optional[Literal[0, 26]] = Field(None, description="Allows only 0 or 26")
#     SelfLifeChanges4: Optional[Literal[0, 27]] = Field(None, description="Allows only 0 or 27")
#     SelfLifeChanges5: Optional[Literal[0, 26]] = Field(None, description="Allows only 0 or 26")
#     SelfLifeChanges6: Optional[Literal[0, 28]] = Field(None, description="Allows only 0 or 28")
#     SelfLifeChangesWork: Optional[str] = Field(None, description="Not applicable field")
#     SelfLifeChanges7: Optional[Literal[0, 51]] = Field(None, description="Allows only 0 or 51")
#     SelfLifeChanges8: Optional[Literal[0, 35]] = Field(None, description="Allows only 0 or 35")
#     SelfLifeChanges9: Optional[Literal[0, 29]] = Field(None, description="Allows only 0 or 29")
#     SelfLifeChanges10: Optional[Literal[0, 21]] = Field(None, description="Allows only 0 or 21")
#     SelfLifeChanges11: Optional[Literal[0, 31]] = Field(None, description="Allows only 0 or 31")
#     SelfLifeChanges12: Optional[Literal[0, 42]] = Field(None, description="Allows only 0 or 42")
#     SelfLifeChanges13: Optional[Literal[0, 32]] = Field(None, description="Allows only 0 or 32")
#     SelfLifeChanges14: Optional[Literal[0, 29]] = Field(None, description="Allows only 0 or 29")
#     SelfLifeChanges15: Optional[Literal[0, 35]] = Field(None, description="Allows only 0 or 35")
#     SelfLifeChanges16: Optional[Literal[0, 35]] = Field(None, description="Allows only 0 or 35")
#     SelfLifeChanges17: Optional[Literal[0, 28]] = Field(None, description="Allows only 0 or 28")
#     SelfLifeChanges18: Optional[Literal[0, 60]] = Field(None, description="Allows only 0 or 60")
#     SelfLifeChanges19: Optional[Literal[0, 52]] = Field(None, description="Allows only 0 or 52")
#     SelfLifeChanges20: Optional[Literal[0, 68]] = Field(None, description="Allows only 0 or 68")
#     SelfLifeChanges21: Optional[Literal[0, 79]] = Field(None, description="Allows only 0 or 79")
#     SelfLifeChanges22: Optional[Literal[0, 18]] = Field(None, description="Allows only 0 or 18")
#     SelfLifeChangesHome: Optional[str] = Field(None, description="Not applicable field")
#     SelfLifeChanges23: Optional[Literal[0, 42]] = Field(None, description="Allows only 0 or 42")
#     SelfLifeChanges24: Optional[Literal[0, 25]] = Field(None, description="Allows only 0 or 25")
#     SelfLifeChanges25: Optional[Literal[0, 47]] = Field(None, description="Allows only 0 or 47")
#     SelfLifeChanges26: Optional[Literal[0, 25]] = Field(None, description="Allows only 0 or 25")
#     SelfLifeChanges27: Optional[Literal[0, 55]] = Field(None, description="Allows only 0 or 55")
#     SelfLifeChanges28: Optional[Literal[0, 50]] = Field(None, description="Allows only 0 or 50")
#     SelfLifeChanges29: Optional[Literal[0, 67]] = Field(None, description="Allows only 0 or 67")
#     SelfLifeChanges31: Optional[Literal[0, 66]] = Field(None, description="Allows only 0 or 66")
#     SelfLifeChanges32: Optional[Literal[0, 65]] = Field(None, description="Allows only 0 or 65")
#     SelfLifeChanges33: Optional[Literal[0, 59]] = Field(None, description="Allows only 0 or 59")
#     SelfLifeChanges34: Optional[Literal[0, 46]] = Field(None, description="Allows only 0 or 46")
#     SelfLifeChanges35: Optional[Literal[0, 41]] = Field(None, description="Allows only 0 or 41")
#     SelfLifeChanges36: Optional[Literal[0, 41]] = Field(None, description="Allows only 0 or 41")
#     SelfLifeChanges37: Optional[Literal[0, 45]] = Field(None, description="Allows only 0 or 45")
#     SelfLifeChanges38: Optional[Literal[0, 50]] = Field(None, description="Allows only 0 or 50")
#     SelfLifeChanges39: Optional[Literal[0, 38]] = Field(None, description="Allows only 0 or 38")
#     SelfLifeChanges40: Optional[Literal[0, 59]] = Field(None, description="Allows only 0 or 59")
#     SelfLifeChanges41: Optional[Literal[0, 50]] = Field(None, description="Allows only 0 or 50")
#     SelfLifeChanges42: Optional[Literal[0, 53]] = Field(None, description="Allows only 0 or 53")
#     SelfLifeChanges43: Optional[Literal[0, 76]] = Field(None, description="Allows only 0 or 76")
#     SelfLifeChanges44: Optional[Literal[0, 96]] = Field(None, description="Allows only 0 or 96")
#     SelfLifeChanges45: Optional[Literal[0, 43]] = Field(None, description="Allows only 0 or 43")
#     SelfLifeChanges46: Optional[Literal[0, 119]] = Field(None, description="Allows only 0 or 119")
#     SelfLifeChanges47: Optional[Literal[0, 123]] = Field(None, description="Allows only 0 or 123")
#     SelfLifeChanges48: Optional[Literal[0, 102]] = Field(None, description="Allows only 0 or 102")
#     SelfLifeChanges49: Optional[Literal[0, 100]] = Field(None, description="Allows only 0 or 100")
#     SelfLifeChangesPersonal: Optional[str] = Field(None, description="Not applicable field")
#     SelfLifeChanges50: Optional[Literal[0, 26]] = Field(None, description="Allows only 0 or 26")
#     SelfLifeChanges51: Optional[Literal[0, 38]] = Field(None, description="Allows only 0 or 38")
#     SelfLifeChanges52: Optional[Literal[0, 35]] = Field(None, description="Allows only 0 or 35")
#     SelfLifeChanges53: Optional[Literal[0, 24]] = Field(None, description="Allows only 0 or 24")
#     SelfLifeChanges54: Optional[Literal[0, 29]] = Field(None, description="Allows only 0 or 29")
#     SelfLifeChanges55: Optional[Literal[0, 27]] = Field(None, description="Allows only 0 or 27")
#     SelfLifeChanges56: Optional[Literal[0, 24]] = Field(None, description="Allows only 0 or 24")
#     SelfLifeChanges57: Optional[Literal[0, 37]] = Field(None, description="Allows only 0 or 37")
#     SelfLifeChanges58: Optional[Literal[0, 45]] = Field(None, description="Allows only 0 or 45")
#     SelfLifeChanges59: Optional[Literal[0, 39]] = Field(None, description="Allows only 0 or 39")
#     SelfLifeChanges61: Optional[Literal[0, 47]] = Field(None, description="Allows only 0 or 47")
#     SelfLifeChanges62: Optional[Literal[0, 48]] = Field(None, description="Allows only 0 or 48")
#     SelfLifeChanges63: Optional[Literal[0, 20]] = Field(None, description="Allows only 0 or 20")
#     SelfLifeChanges65: Optional[Literal[0, 70]] = Field(None, description="Allows only 0 or 70")
#     SelfLifeChanges66: Optional[Literal[0, 51]] = Field(None, description="Allows only 0 or 51")
#     SelfLifeChanges67: Optional[Literal[0, 36]] = Field(None, description="Allows only 0 or 36")
#     SelfLifeChangesFinancial: Optional[str] = Field(None, description="Not applicable field")
#     SelfLifeChanges68: Optional[Literal[0, 38]] = Field(None, description="Allows only 0 or 38")
#     SelfLifeChanges69: Optional[Literal[0, 60]] = Field(None, description="Allows only 0 or 60")
#     SelfLifeChanges70: Optional[Literal[0, 56]] = Field(None, description="Allows only 0 or 56")
#     SelfLifeChanges71: Optional[Literal[0, 43]] = Field(None, description="Allows only 0 or 43")
#     SelfLifeChanges72: Optional[Literal[0, 20]] = Field(None, description="Allows only 0 or 20")
#     SelfLifeChanges73: Optional[Literal[0, 37]] = Field(None, description="Allows only 0 or 37")
#     SelfLifeChanges74: Optional[Literal[0, 58]] = Field(None, description="Allows only 0 or 58")


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
