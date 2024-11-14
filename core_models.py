from pydantic import BaseModel, Extra, Field, field_validator, model_validator, ValidationError
from typing import Optional, Union, List, Set, Any, Literal
from datetime import datetime
import ast


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
    last_updated: datetime


class LevanteVariant(VariantBase):
    pass


class RoarVariant(VariantBase):
    pass


class TrialBase(BaseModel):
    validation_err_msg: Optional[str] = ""
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

    @model_validator(mode='after')
    def check_rt(self):
        rt_err = []
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
                        rt_err.append(f"rt less than {rt_min / 1000}s.")
                    elif self.rt > rt_max:
                        rt_err.append(f"rt exceeds {rt_max / 1000}s.")
                elif isinstance(self.rt, str):
                    try:
                        rt_dict = ast.literal_eval(self.rt)
                        if not all([value > rt_min for value in rt_dict.values()]):
                            rt_err.append(f"rt less than {rt_min / 1000}s.")
                        if not all([value < rt_max for value in rt_dict.values()]):
                            rt_err.append(f"rt exceeds {rt_max / 1000}s.")
                    except Exception as e:
                        rt_err.append(f"rt string converted to dict failed. {e}")

            else:
                rt_err.append("rt is missing.")

        if rt_err:
            if self.validation_err_msg:
                self.validation_err_msg = f"{self.validation_err_msg}, {str(rt_err)}"
            else:
                self.validation_err_msg = str(rt_err)

        return self

    @model_validator(mode='after')
    def check_trial_index(self):
        trial_index_err = []

        if self.trial_index:
            if not isinstance(self.trial_index, int):
                trial_index_err.append("trial_index needs to be int type.")
        else:
            trial_index_err.append("trial_index is missing.")

        if trial_index_err:
            if self.validation_err_msg:
                self.validation_err_msg = f"{self.validation_err_msg}, {str(trial_index_err)}"
            else:
                self.validation_err_msg = str(trial_index_err)

        return self


class RunBase(BaseModel):
    validation_err_msg: Optional[str] = ""
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

    _trials: Optional[list[LevanteTrial]] = []

    def add_levante_trial(self, trial: LevanteTrial):
        self._trials.append(trial)

    def check_trials_count(self):
        trials_count_err = []

        if len(self._trials) < 3:
            trials_count_err.append("Less than 3 trials in this run.")

        if trials_count_err:
            if self.validation_err_msg:
                self.validation_err_msg = f"{self.validation_err_msg}, {str(trials_count_err)}"
            else:
                self.validation_err_msg = str(trials_count_err)

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

        self.validation_err_msg = (f"response_locations are {str(response_location)}, "
                                   f"has consecutive response_location of 5: {has_consecutive_identical(response_location, 5)}, "
                                   f"{self.validation_err_msg}")

    def validate_trials_in_run(self):
        self.check_trials_count()
        self.check_straight_line_trials()


class UserBase(BaseModel):
    validation_err_msg: Optional[str] = ""
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
        birth_year_month_err = []
        if self.user_type == 'student':
            if self.birth_year and self.birth_month and isinstance(self.birth_year, int) and isinstance(
                    self.birth_month, int):
                if self.birth_month not in range(1, 13):
                    birth_year_month_err.append("Birth Month not in between 1 and 12.")
                if self.birth_year < 2000:
                    birth_year_month_err.append("Birth Year is prior than 2000.")
            else:
                birth_year_month_err.append("Birth Year and Month missing or not integer.")

        if birth_year_month_err:
            if self.validation_err_msg:
                self.validation_err_msg = f"{self.validation_err_msg}, {str(birth_year_month_err)}"
            else:
                self.validation_err_msg = str(birth_year_month_err)

        return self


class SurveyResponse(BaseModel):
    survey_response_id: str
    user_id: str
    created_at: datetime


class StudentSurveyResponse(SurveyResponse):
    Example1Comic: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    Example2Neat: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherNice: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherLike: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherListen: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    SchoolFun: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    SchoolEnjoy: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    SchoolHappy: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    SchoolSafe: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    ClassFriends: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    ClassHelp: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    ClassPlay: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    ClassNice: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    LonelySchool: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    LearningGood: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    SchoolGiveUp: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    ReadingEnjoy: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    MathEnjoy: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    ReadingGood: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    MathGood: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    GrowthMindSmart: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    GrowthMindRead: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    GrowthMindMath: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)


class TeacherSurveyResponse(SurveyResponse):
    TeacherAge: Optional[int] = Field(None, description="Accepts only 10-100 as valid inputs", ge=10, le=100)
    TeacherGender: Optional[str] = ""
    TeacherGenderComment: Optional[str] = ""
    TeacherEducation: Optional[str] = ""
    TeacherUndergrad: Optional[str] = ""
    TeacherUndergradOtherEd: Optional[str] = ""
    TeacherUndergradNonEd: Optional[str] = ""
    TeacherUndergradOther: Optional[str] = ""
    TeacherGrad: Optional[str] = ""
    TeacherGradOtherEd: Optional[str] = ""
    TeacherGradNonEd: Optional[str] = ""
    TeacherGradOther: Optional[str] = ""
    TeacherYears: Optional[str] = ""
    TeacherYearsSchool: Optional[str] = ""
    TeacherClimate1: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherClimate2: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherClimate3: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherClimate4: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherClimate5: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherClimate6: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherClimate7a: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherClimate7b: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherClimate8: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherClimate9: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherClimate10: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)

    TeacherFeelJob1: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
    TeacherFeelJob2: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
    TeacherFeelJob3: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
    TeacherFeelJob5: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
    TeacherFeelJob6: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
    TeacherFeelJob7: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
    TeacherFeelJob9: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
    TeacherFeelJob10: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
    TeacherFeelJob12: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
    TeacherFeelJob13: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
    TeacherFeelJob14: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
    TeacherFeelJob15: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)
    TeacherFeelJob16: Optional[int] = Field(None, description="Accepts only 0-6 as valid inputs", ge=0, le=6)

    TeacherBeliefTeach1: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
    TeacherBeliefTeach2: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
    TeacherBeliefTeach3: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
    TeacherBeliefTeach4: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
    TeacherBeliefTeach6: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
    TeacherBeliefTeach7: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
    TeacherBeliefTeach8: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
    TeacherBeliefTeach9: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
    TeacherBeliefTeach10: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
    TeacherBeliefTeach11: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
    TeacherBeliefTeach12: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
    TeacherBeliefTeach5: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)

    TeacherIdeasChildren1: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren2: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren3: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren4: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren5: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren6: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren7: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren8: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren9: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren10: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren11: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren12: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren13: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren14: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren15: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)
    TeacherIdeasChildren16: Optional[int] = Field(None, description="Accepts only 0-3 as valid inputs", ge=0, le=3)

    TeacherNumberClasses: Optional[int] = Field(None, description="Accepts only 1-7 as valid inputs", ge=1, le=7)
    TeacherOtherAdult: Optional[str] = ""
    TeacherStudentsEnroll: Optional[int] = None
    TeacherStudentsGender: Optional[str] = ""
    TeacherStudentsGenderGirls: Optional[int] = None
    TeacherStudentsGenderBoys: Optional[int] = None
    TeacherStudentsGenderNonbinary: Optional[int] = None
    TeacherStudentsGenderOther: Optional[int] = None
    TeacherStudentsAges: Optional[str] = ""
    TeacherStudentsAges5: Optional[int] = None
    TeacherStudentsAges6: Optional[int] = None
    TeacherStudentsAges7: Optional[int] = None
    TeacherStudentsAges8: Optional[int] = None
    TeacherStudentsAges9: Optional[int] = None
    TeacherStudentsAges10: Optional[int] = None
    TeacherStudentsAges11: Optional[int] = None
    TeacherStudentsAges12: Optional[int] = None
    TeacherStudentsAges13: Optional[int] = None
    TeacherMultigrade: Optional[str] = ""
    TeacherMultigradeLevels: Optional[str] = ""
    TeacherMultigradeLevelsComment: Optional[str] = ""
    TeacherGradeLevel: Optional[str] = ""
    TeacherGradeLevelComment: Optional[str] = ""
    TeacherLanguageStudents: Optional[str] = ""
    TeacherLanguageStudentsComment: Optional[str] = ""
    TeacherLanguageTeachers: Optional[str] = ""
    TeacherLanguageTeachersComment: Optional[str] = ""
    TeacherReadingBelow: Optional[int] = None
    TeacherReadingAbove: Optional[int] = None
    TeacherMathBelow: Optional[int] = None
    TeacherMathAbove: Optional[int] = None
    TeacherReadingFrequency: Optional[str] = ""
    TeacherReadingTime: Optional[str] = ""
    TeacherMathFrequency: Optional[str] = ""
    TeacherMathTime: Optional[str] = ""
    TeacherFamilySupport: Optional[str] = ""
    TeacherFamilyOverwhelm: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
    TeacherFamilyHelping: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
    TeacherComm: Optional[str] = ""
    TeacherCommFamily: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)
    TeacherCommStudents: Optional[int] = Field(None, description="Accepts only 0-4 as valid inputs", ge=0, le=4)


class CaregiverSurveyResponse(SurveyResponse):
    CaregiverSurveyIntroA: Optional[str] = ""
    CaregiverSectionIntro1: Optional[str] = ""
    RespondentRelationship: Optional[str] = ""
    RespondentRelationshipComment: Optional[str] = ""
    RespondentRelationshipOtherSpecify: Optional[str] = ""
    RespondentTimeCaring: Optional[str] = ""
    RespondentTimeCaringYears: Optional[str] = ""
    RespondentTimeCaringMonths: Optional[str] = ""
    ChildAgeYears: Optional[str] = ""
    ChildHeightCurrent: Optional[str] = ""
    ChildHeightCurrentFeet: Optional[str] = ""
    ChildHeightCurrentInches: Optional[str] = ""
    ChildWeightCurrent: Optional[str] = ""
    ChildWeightCurrentPounds: Optional[str] = ""
    ChildBornEarly: Optional[str] = ""
    ChildWeightBirth: Optional[str] = ""
    ChildWeightBirthPounds: Optional[str] = ""
    ChildWeightBirthOunces: Optional[str] = ""
    ChildHealth: Optional[str] = ""
    ChildTeeth: Optional[str] = ""
    ChildPhysicalActivity: Optional[str] = ""
    ChildChronicHealth: Optional[str] = ""
    ChildBreathing: Optional[str] = ""
    ChildEating: Optional[str] = ""
    ChildDigestion: Optional[str] = ""
    ChildPain: Optional[str] = ""
    ChildToothache: Optional[str] = ""
    ChildGums: Optional[str] = ""
    ChildCavities: Optional[str] = ""
    ChildHealthOther: Optional[str] = ""
    ChildConcentration: Optional[str] = ""
    ChildStairs: Optional[str] = ""
    ChildDressBathe: Optional[str] = ""
    ChildHearing: Optional[str] = ""
    ChildEyesight: Optional[str] = ""
    ChildVisionHearingOther: Optional[str] = ""
    ChildVisionHearingExp: Optional[str] = ""
    ChildGenetic: Optional[str] = ""
    ChildGeneticExp: Optional[str] = ""
    ChildHealthConditions: Optional[str] = ""
    ChildConduct: Optional[str] = ""
    ChildConductCurrent: Optional[str] = ""
    ChildConductLevel: Optional[str] = ""
    ChildDevDelay: Optional[str] = ""
    ChildDevDelayCurrent: Optional[str] = ""
    ChildDevDelayLevel: Optional[str] = ""
    ChildIntellDis: Optional[str] = ""
    ChildIntellDisCurrent: Optional[str] = ""
    ChildIntellDisLevel: Optional[str] = ""
    ChildSpeechLang: Optional[str] = ""
    ChildSpeechLangCurrent: Optional[str] = ""
    ChildSpeechLangLevel: Optional[str] = ""
    ChildLearnDis: Optional[str] = ""
    ChildLearnDisCurrent: Optional[str] = ""
    ChildLearnDisLevel: Optional[str] = ""
    ChildAutism: Optional[str] = ""
    ChildAutismCurrent: Optional[str] = ""
    ChildAutismLevel: Optional[str] = ""
    ChildADHD: Optional[str] = ""
    ChildADHDCurrent: Optional[str] = ""
    ChildADHDLevel: Optional[str] = ""
    ChildSleep: Optional[str] = ""
    ChildSleepHabits: Optional[str] = ""
    ChildSleep1: Optional[str] = ""
    ChildSleep2: Optional[str] = ""
    ChildSleep5: Optional[str] = ""
    ChildSleepHours: Optional[str] = ""
    ChildSexBirth: Optional[str] = ""
    ChildGenderIntro: Optional[str] = ""
    ChildGenderCurrent: Optional[str] = ""
    ChildGenderCurrentComment: Optional[str] = ""
    ChildBehGirls: Optional[str] = ""
    ChildBeGirl: Optional[str] = ""
    ChildBehBoys: Optional[str] = ""
    ChildBeBoy: Optional[str] = ""
    ChildPubertyYN: Optional[str] = ""
    ChildPubertyFemale: Optional[str] = ""
    ChildPubertyGrowthFemale: Optional[str] = ""
    ChildPubertyHairFemale: Optional[str] = ""
    ChildPubertySkinFemale: Optional[str] = ""
    ChildPubertyBreastFemale: Optional[str] = ""
    ChildPubertyMenstruate: Optional[str] = ""
    ChildPubertyMenstruateAge: Optional[str] = ""
    ChildPubertyMale: Optional[str] = ""
    ChildPubertyGrowthMale: Optional[str] = ""
    ChildPubertyHairMale: Optional[str] = ""
    ChildPubertySkinMale: Optional[str] = ""
    ChildPubertyVoiceMale: Optional[str] = ""
    ChildPubertyFaceHairMale: Optional[str] = ""
    CaregiverSectionIntro2: Optional[str] = ""
    ChildSDQitems: Optional[str] = ""
    ChildSDQ2: Optional[str] = ""
    ChildSDQ3: Optional[str] = ""
    ChildSDQ6: Optional[str] = ""
    ChildSDQ8: Optional[str] = ""
    ChildSDQ10: Optional[str] = ""
    ChildSDQ11: Optional[str] = ""
    ChildSDQ13: Optional[str] = ""
    ChildSDQ14: Optional[str] = ""
    ChildSDQ15: Optional[str] = ""
    ChildSDQ16: Optional[str] = ""
    ChildSDQ17: Optional[str] = ""
    ChildSDQ18: Optional[str] = ""
    ChildSDQ22: Optional[str] = ""
    ChildSDQ23: Optional[str] = ""
    ChildSDQ24: Optional[str] = ""
    ChildSCSitems: Optional[str] = ""
    ChildSCS1: Optional[str] = ""
    ChildSCS2: Optional[str] = ""
    ChildSCS3: Optional[str] = ""
    ChildSCS4: Optional[str] = ""
    ChildSCS5: Optional[str] = ""
    ChildSCS6: Optional[str] = ""
    ChildSCS7: Optional[str] = ""
    ChildSCS8: Optional[str] = ""
    ChildSCS9: Optional[str] = ""
    ChildSCS10: Optional[str] = ""
    ChildSCS11: Optional[str] = ""
    ChildSCS12: Optional[str] = ""
    ChildCBQitems: Optional[str] = ""
    ChildCBQ1: Optional[str] = ""
    ChildCBQ2: Optional[str] = ""
    ChildCBQ3: Optional[str] = ""
    ChildCBQ4: Optional[str] = ""
    ChildCBQ5: Optional[str] = ""
    ChildCBQ6: Optional[str] = ""
    ChildCBQ7: Optional[str] = ""
    ChildCBQ9: Optional[str] = ""
    ChildCBQ10: Optional[str] = ""
    ChildCBQ11: Optional[str] = ""
    ChildJukesItems: Optional[str] = ""
    ChildJukes1: Optional[str] = ""
    ChildJukes2: Optional[str] = ""
    ChildJukes3: Optional[str] = ""
    ChildJukes4: Optional[str] = ""
    ChildJukes5: Optional[str] = ""
    ChildJukes6: Optional[str] = ""
    ChildJukes7: Optional[str] = ""
    ChildJukes8: Optional[str] = ""
    ChildJukes9: Optional[str] = ""
    ChildJukes10: Optional[str] = ""
    ChildJukes11: Optional[str] = ""
    ChildJukes12: Optional[str] = ""
    ChildJukes13: Optional[str] = ""
    ChildJukes14: Optional[str] = ""
    ChildJukes15: Optional[str] = ""
    ChildJukes16: Optional[str] = ""
    ChildJukes17: Optional[str] = ""
    ChildJukes18: Optional[str] = ""
    ChildJukes19: Optional[str] = ""
    ChildJukes20: Optional[str] = ""
    ChildFriends: Optional[str] = ""
    ChildBullied: Optional[str] = ""
    ChildBullyOthers: Optional[str] = ""
    CaregiverSectionIntro3: Optional[str] = ""
    ChildELSitems: Optional[str] = ""
    ChildELS1: Optional[str] = ""
    ChildELS2: Optional[str] = ""
    ChildELS3: Optional[str] = ""
    ChildELS4: Optional[str] = ""
    ChildELS5: Optional[str] = ""
    ChildELS6: Optional[str] = ""
    ChildELS7: Optional[str] = ""
    ChildELS8: Optional[str] = ""
    ChildELS9: Optional[str] = ""
    ChildELS10: Optional[str] = ""
    ChildELS11: Optional[str] = ""
    ChildELS12: Optional[str] = ""
    ChildELS13: Optional[str] = ""
    ChildELS14: Optional[str] = ""
    ChildELS15: Optional[str] = ""
    ChildELS16: Optional[str] = ""
    ChildELS17: Optional[str] = ""
    ChildELS18: Optional[str] = ""
    ChildAttendEC: Optional[str] = ""
    ChildAttendKinder: Optional[str] = ""
    ChildAttendECKinderTime: Optional[str] = ""
    ChildAttendPrimaryAge: Optional[str] = ""
    ChildEdAchieve: Optional[str] = ""
    ChildSchoolItems: Optional[str] = ""
    ChildNSCHg10a: Optional[str] = ""
    ChildNSCHg10b: Optional[str] = ""
    ChildNSCHg10c: Optional[str] = ""
    ChildNSCHg10d: Optional[str] = ""
    ChildNSCHg10e: Optional[str] = ""
    ChildDeviceUse: Optional[str] = ""
    ChildPhone: Optional[str] = ""
    ChildPhoneAge: Optional[str] = ""
    ChildSocialMedia: Optional[str] = ""
    ChildEFQ: Optional[str] = ""
    ChildEFQ1: Optional[str] = ""
    ChildEFQ2: Optional[str] = ""
    ChildEFQ3: Optional[str] = ""
    ChildEFQ4: Optional[str] = ""
    ChildEFQ5: Optional[str] = ""
    ChildEFQ6: Optional[str] = ""
    ChildEFQ7: Optional[str] = ""
    ChildEFQ8: Optional[str] = ""
    ChildEFQ9: Optional[str] = ""
    ChildEFQ10: Optional[str] = ""
    ChildEFQ11: Optional[str] = ""
    ChildEFQ12: Optional[str] = ""
    ChildEFQ13: Optional[str] = ""
    ChildEFQ14: Optional[str] = ""
    ChildEFQ15: Optional[str] = ""
    ChildEFQ16: Optional[str] = ""
    ChildEFQ17: Optional[str] = ""
    ChildEFQ18: Optional[str] = ""
    ChildEFQ19: Optional[str] = ""
    ChildEFQ20: Optional[str] = ""
    ChildEFQ21: Optional[str] = ""
    ChildEFQ22: Optional[str] = ""
    ChildEFQ23: Optional[str] = ""
    ChildEFQ24: Optional[str] = ""
    ChildEFQ25: Optional[str] = ""
    ChildEFQ26: Optional[str] = ""
    ChildEFQ27: Optional[str] = ""
    ChildEFQ28: Optional[str] = ""
    ChildEFQ29: Optional[str] = ""
    ChildEFQ30: Optional[str] = ""
    ChildEFQ31: Optional[str] = ""
    ChildEFQ32: Optional[str] = ""
    ChildEFQ33: Optional[str] = ""
    ChildEFQ34: Optional[str] = ""
    CaregiverSectionIntro4: Optional[str] = ""
    HomeHOME1: Optional[str] = ""
    HomeHOME2: Optional[str] = ""
    HomeHOME3: Optional[str] = ""
    HomeHOME4: Optional[str] = ""
    HomeHOME5: Optional[str] = ""
    HomeHOME6: Optional[str] = ""
    HomeHOME7: Optional[str] = ""
    HomeHOME9: Optional[str] = ""
    HomeHOME15: Optional[str] = ""
    HomeHOME23: Optional[str] = ""
    HomeHOME22: Optional[str] = ""
    HomeHOME24: Optional[str] = ""
    HomeHOME24a: Optional[str] = ""
    HomeHOME24b: Optional[str] = ""
    HomeHOME24c: Optional[str] = ""
    HomeHOME24d: Optional[str] = ""
    HomeHOME24e: Optional[str] = ""
    HomeHOME24f: Optional[str] = ""
    HomeHOME24g: Optional[str] = ""
    HomeHOME24h: Optional[str] = ""
    HomeHOME24i: Optional[str] = ""
    HomeHOME24j: Optional[str] = ""
    HomeHOME25: Optional[str] = ""
    HomeHOME26: Optional[str] = ""
    HomeHOME27: Optional[str] = ""
    HomeHOME28: Optional[str] = ""
    HomeHOME29: Optional[str] = ""
    HomeHOME30: Optional[str] = ""
    HomeHOME31: Optional[str] = ""
    HomeHOME32: Optional[str] = ""
    HomeHOME33: Optional[str] = ""
    HomeHOME34: Optional[str] = ""
    HomeHOME35: Optional[str] = ""
    HomeHOME36: Optional[str] = ""
    HomeHOME37: Optional[str] = ""
    HomeHOME38: Optional[str] = ""
    SelfParentStressNCSHh9: Optional[str] = ""
    SelfParentStressNSCHh9a: Optional[str] = ""
    SelfParentStressNSCHh9b: Optional[str] = ""
    SelfParentStressNSCHh9c: Optional[str] = ""
    SelfParentRohner: Optional[str] = ""
    SelfParentRohnerC3: Optional[str] = ""
    SelfParentRohnerC7: Optional[str] = ""
    SelfParentRohnerC14: Optional[str] = ""
    SelfParentRohnerW15: Optional[str] = ""
    SelfParentRohnerC20: Optional[str] = ""
    SelfParentRohnerW21: Optional[str] = ""
    SelfParentRohnerW23: Optional[str] = ""
    SelfParentRohnerC26: Optional[str] = ""
    SelfParentRohnerW27: Optional[str] = ""
    SelfParentRohnerW29: Optional[str] = ""
    SelfParentMICS: Optional[str] = ""
    SelfParentMICS1: Optional[str] = ""
    SelfParentMICS2: Optional[str] = ""
    SelfParentMICS4: Optional[str] = ""
    SelfParentMICS6: Optional[str] = ""
    SelfParentMICS12: Optional[str] = ""
    CaregiverSurveyIntroB: Optional[str] = ""
    HomeNeighborhood: Optional[str] = ""
    HomeNSCHi7a: Optional[str] = ""
    HomeNSCHi7b: Optional[str] = ""
    HomeNSCHi7c: Optional[str] = ""
    HomeNSCHi7d: Optional[str] = ""
    HomeNSCHi7e: Optional[str] = ""
    HomeNSCHi7f: Optional[str] = ""
    HomeNSCHi7g: Optional[str] = ""
    HomeNSCHi8items: Optional[str] = ""
    HomeNSCHi8a: Optional[str] = ""
    HomeNSCHi8b: Optional[str] = ""
    HomeNSCHi8c: Optional[str] = ""
    HomeNSCHi8d: Optional[str] = ""
    HomeNSCHi8e: Optional[str] = ""
    HomeFood: Optional[str] = ""
    HomeFoodLast: Optional[str] = ""
    HomeFoodBalanced: Optional[str] = ""
    HomeFoodSkipMeal: Optional[str] = ""
    HomeFoodSkipMealAmount: Optional[str] = ""
    HomeCHAOS: Optional[str] = ""
    HomeCHAOS4: Optional[str] = ""
    HomeCHAOS10: Optional[str] = ""
    HomeCHAOS14: Optional[str] = ""
    HomeCHAOS15: Optional[str] = ""
    SelfParentStressNSCHh8: Optional[str] = ""
    SelfAnxDep: Optional[str] = ""
    SelfAnxDep1: Optional[str] = ""
    SelfAnxDep2: Optional[str] = ""
    SelfAnxDep3: Optional[str] = ""
    SelfAnxDep4: Optional[str] = ""
    SelfSupport: Optional[str] = ""
    SelfSupport1: Optional[str] = ""
    SelfSupport2: Optional[str] = ""
    SelfSupport3: Optional[str] = ""
    SelfSupport4: Optional[str] = ""
    SelfSupport5: Optional[str] = ""
    SelfSupport6: Optional[str] = ""
    SelfSupport7: Optional[str] = ""
    SelfSupport8: Optional[str] = ""
    SelfSupport9: Optional[str] = ""
    SelfSupport10: Optional[str] = ""
    SelfSupport11: Optional[str] = ""
    SelfSupport12: Optional[str] = ""
    SelfDiscrim: Optional[str] = ""
    SelfDiscrim1: Optional[str] = ""
    SelfDiscrim2: Optional[str] = ""
    SelfDiscrim3: Optional[str] = ""
    SelfDiscrim4: Optional[str] = ""
    SelfDiscrim5: Optional[str] = ""
    SelfLifeChanges: Optional[str] = ""
    SelfLifeChangesHealth: Optional[str] = ""
    SelfLifeChanges1: Optional[str] = ""
    SelfLifeChanges2: Optional[str] = ""
    SelfLifeChanges3: Optional[str] = ""
    SelfLifeChanges4: Optional[str] = ""
    SelfLifeChanges5: Optional[str] = ""
    SelfLifeChanges6: Optional[str] = ""
    SelfLifeChangesWork: Optional[str] = ""
    SelfLifeChanges7: Optional[str] = ""
    SelfLifeChanges8: Optional[str] = ""
    SelfLifeChanges9: Optional[str] = ""
    SelfLifeChanges10: Optional[str] = ""
    SelfLifeChanges11: Optional[str] = ""
    SelfLifeChanges12: Optional[str] = ""
    SelfLifeChanges13: Optional[str] = ""
    SelfLifeChanges14: Optional[str] = ""
    SelfLifeChanges15: Optional[str] = ""
    SelfLifeChanges16: Optional[str] = ""
    SelfLifeChanges17: Optional[str] = ""
    SelfLifeChanges18: Optional[str] = ""
    SelfLifeChanges19: Optional[str] = ""
    SelfLifeChanges20: Optional[str] = ""
    SelfLifeChanges21: Optional[str] = ""
    SelfLifeChanges22: Optional[str] = ""
    SelfLifeChangesHome: Optional[str] = ""
    SelfLifeChanges23: Optional[str] = ""
    SelfLifeChanges24: Optional[str] = ""
    SelfLifeChanges25: Optional[str] = ""
    SelfLifeChanges26: Optional[str] = ""
    SelfLifeChanges27: Optional[str] = ""
    SelfLifeChanges28: Optional[str] = ""
    SelfLifeChanges29: Optional[str] = ""
    SelfLifeChanges31: Optional[str] = ""
    SelfLifeChanges32: Optional[str] = ""
    SelfLifeChanges33: Optional[str] = ""
    SelfLifeChanges34: Optional[str] = ""
    SelfLifeChanges35: Optional[str] = ""
    SelfLifeChanges36: Optional[str] = ""
    SelfLifeChanges37: Optional[str] = ""
    SelfLifeChanges38: Optional[str] = ""
    SelfLifeChanges39: Optional[str] = ""
    SelfLifeChanges40: Optional[str] = ""
    SelfLifeChanges41: Optional[str] = ""
    SelfLifeChanges42: Optional[str] = ""
    SelfLifeChanges43: Optional[str] = ""
    SelfLifeChanges44: Optional[str] = ""
    SelfLifeChanges45: Optional[str] = ""
    SelfLifeChanges46: Optional[str] = ""
    SelfLifeChanges47: Optional[str] = ""
    SelfLifeChanges48: Optional[str] = ""
    SelfLifeChanges49: Optional[str] = ""
    SelfLifeChangesPersonal: Optional[str] = ""
    SelfLifeChanges50: Optional[str] = ""
    SelfLifeChanges51: Optional[str] = ""
    SelfLifeChanges52: Optional[str] = ""
    SelfLifeChanges53: Optional[str] = ""
    SelfLifeChanges54: Optional[str] = ""
    SelfLifeChanges55: Optional[str] = ""
    SelfLifeChanges56: Optional[str] = ""
    SelfLifeChanges57: Optional[str] = ""
    SelfLifeChanges58: Optional[str] = ""
    SelfLifeChanges59: Optional[str] = ""
    SelfLifeChanges61: Optional[str] = ""
    SelfLifeChanges62: Optional[str] = ""
    SelfLifeChanges63: Optional[str] = ""
    SelfLifeChanges65: Optional[str] = ""
    SelfLifeChanges66: Optional[str] = ""
    SelfLifeChanges67: Optional[str] = ""
    SelfLifeChangesFinancial: Optional[str] = ""
    SelfLifeChanges68: Optional[str] = ""
    SelfLifeChanges69: Optional[str] = ""
    SelfLifeChanges70: Optional[str] = ""
    SelfLifeChanges71: Optional[str] = ""
    SelfLifeChanges72: Optional[str] = ""
    SelfLifeChanges73: Optional[str] = ""
    SelfLifeChanges74: Optional[str] = ""


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
