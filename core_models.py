from pydantic import BaseModel, Extra, Field, field_validator, model_validator, ValidationError
from typing import Optional, Union, List, Set, Any, Literal
from datetime import datetime, timezone
import ast
from scipy.stats import binom, binomtest
from math import isnan


class DistrictBase(BaseModel):
    district_id: str
    abbreviation: Optional[str] = None
    name: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class SchoolBase(BaseModel):
    school_id: str
    district_id: str
    abbreviation: Optional[str] = None
    name: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ClassBase(BaseModel):
    class_id: str
    school_id: str
    district_id: str
    abbreviation: Optional[str] = None
    name: str
    grade: Optional[Union[str, int]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class GroupBase(BaseModel):
    group_id: str
    name: str
    abbreviation: Optional[str] = None
    tags: Optional[str] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None


class TaskBase(BaseModel):
    task_id: str
    name: str
    description: Optional[str] = None
    last_updated: datetime


class VariantBase(BaseModel):
    variant_id: str
    task_id: str
    name: Optional[str] = None
    button_layout: Optional[str] = None
    corpus: Optional[str] = None
    key_helpers: Optional[bool] = None
    language: Optional[str] = None
    adaptive: bool = False
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

    @field_validator("language", mode="before")
    def normalize_language(cls, v):
        """
        Normalize language codes:
        - en -> en-US
        - es -> es-CO
        - de -> de-DE
        - others -> Other(<original>)
        """
        if v is None:
            return None

        code = str(v).strip()
        if code == "":
            return None

        lower = code.lower()
        if lower == "en":
            return "en-US"
        if lower == "es":
            return "es-CO"
        if lower == "de":
            return "de-DE"

        # everything else
        return f"Other({code})"

    @model_validator(mode="after")
    def set_adaptive_from_name(self):
        """
        Set adaptive=True if the variant name contains 'adaptive' (case-insensitive).
        """
        name = (self.name or "").lower()
        self.adaptive = "adaptive" in name
        return self


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
    rt_numeric: Optional[int] = None
    server_timestamp: Optional[datetime] = None


class LevanteTrial(TrialBase):
    is_practice_trial: Optional[bool] = None
    corpus_id: Optional[str] = None
    corpus_trial_type: Optional[Any] = None
    response_type: Optional[str] = None
    response_location: Optional[Any] = None
    distractors: Optional[str] = None
    trial_mode: Optional[str] = None

    # For some roar tasks
    theta_estimate: Optional[float] = None  # Union[float, str]
    theta_estimate2: Optional[float] = None
    theta_se: Optional[float] = None
    theta_se2: Optional[float] = None

    valid_trial: Optional[bool] = None
    validation_msg_trial: Optional[str] = None
    warning_msg_trial: Optional[str] = None

    @field_validator("response", mode="before")
    def normalize_response_nan(cls, v):
        """
        Convert 'nan' strings or float('nan') to None (Firestore NULL).
        """
        if v is None:
            return None

        # Handle string "nan" / "NaN"
        if isinstance(v, str) and v.strip().lower() == "nan":
            return None

        # Handle float('nan')
        if isinstance(v, float) and isnan(v):
            return None

        return v

    @model_validator(mode='after')
    def set_is_practice_from_stage(self):
        stage = str(self.assessment_stage or "").lower()
        ctype = str(self.corpus_trial_type or "").lower()

        if (
                "practice" in stage
                or "training" in stage
                or "practice" in ctype
                # or "training" in ctype
        ):
            self.is_practice_trial = True

        return self

    @model_validator(mode='after')
    def check_rt(self):
        rt_min = 100
        rt_max = 10000
        msg = []

        if self not in ['instructions', 'practice_response']:
            if self.rt not in ["", "{}", "0", 0]:
                if isinstance(self.rt, int):
                    if self.task_id in ['matrix-reasoning']:
                        rt_min = 300
                        rt_max = 60000
                    elif self.task_id in ['egma-math']:
                        rt_max = 60000

                    if self.rt < rt_min:
                        msg.append(f"fast_rt_{rt_min / 1000}s")
                    elif self.rt > rt_max:
                        msg.append(f"slow_rt_{rt_max / 1000}s")
                elif isinstance(self.rt, str):
                    try:
                        rt_dict = ast.literal_eval(self.rt)
                        if not all([value > rt_min for value in rt_dict.values()]):
                            msg.append(f"fast_rt_{rt_min / 1000}s")
                        if not all([value < rt_max for value in rt_dict.values()]):
                            msg.append(f"slow_rt_{rt_max / 1000}s")
                    except Exception as e:
                        msg.append(f"rt string converted to dict failed as {e}")

            else:
                msg.append("rt_missing")
        if msg:
            self.validation_msg_trial = ";".join(msg)
        return self

    @model_validator(mode="after")
    def set_rt_numeric(self):
        """
        Normalize rt into an integer (truncate decimals).
        If conversion fails, rt_numeric is set to None.
        """
        v = self.rt

        # Handle obvious null / empty cases
        if v in (None, "", "{}", "0", 0):
            self.rt_numeric = None
            return self

        # numeric types
        if isinstance(v, (int, float)):
            if isinstance(v, float) and isnan(v):
                self.rt_numeric = None
            else:
                # 🔹 Truncate decimals
                self.rt_numeric = int(float(v))
            return self

        # string
        if isinstance(v, str):
            s = v.strip()
            if s in ("", "{}", "0"):
                self.rt_numeric = None
                return self

            try:
                self.rt_numeric = int(float(s))
                return self
            except ValueError:
                self.rt_numeric = None
                return self

        self.rt_numeric = None
        return self

    @model_validator(mode='after')
    def check_trial_index(self):
        if self.trial_index:
            if not isinstance(self.trial_index, int):
                self.warning_msg_trial = f"trial_index_not_int"
        else:
            self.warning_msg_trial = f"trial_index_missing"
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
    age: Optional[float] = None
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
    validation_msg_run: Optional[str] = None
    warning_msg_run: Optional[str] = None
    # bc_score: Optional[str] = None  # e.g., "3/12"
    bc_p_below: Optional[float] = None  # one-tailed p-value P[X <= k]
    # flag_below_chance_0p05: Optional[bool] = None  # True if p <= .05

    _non_practice_trials: Optional[list[LevanteTrial]] = []

    def compute_below_chance_flags_scipy(
            self,
            *,
            default_afc: int = 4,  # 4AFC ⇒ chance p = 0.25
            alpha: float = 0.05,  # flag threshold
            min_trials: int = 8  # avoid noisy flags on tiny runs
    ) -> None:
        """
        Compute a one-tailed binomial 'below-chance' p-value from this run's trials
        and populate three lean fields: bc_score, bc_p_below, flag_below_chance_0p05.

        Uses: self._non_practice_trials (Iterable of trial objects with .correct: bool)
        """

        trials = self._non_practice_trials or []
        n = len(trials)
        k = sum(1 for t in trials if getattr(t, "correct", None) is True)

        # 3 lean columns
        # self.bc_score = f"{k}/{n}"
        self.bc_p_below = None
        # self.flag_below_chance_0p05 = None

        # Not enough data? leave as None
        if n <= 0 or n < min_trials:
            return

        # Resolve chance level (4AFC by default). Replace with self.task_afc / self.n_options if you store them.
        afc = default_afc
        p = 1.0 / float(afc)

        # One-tailed p-value: P[X <= k] for X ~ Binom(n, p)
        self.bc_p_below = float(binomtest(k, n, p, alternative='less').pvalue)
        # self.flag_below_chance_0p05 = (self.bc_p_below <= alpha)

    def add_age_from_users(self, birth_month: int, birth_year: int):
        if not (birth_year and birth_month and self.time_started):
            return None
        birth_date = datetime(
            birth_year,
            birth_month,
            15,  # assume middle of the month
            tzinfo=timezone.utc
        )
        run_date = self.time_started.astimezone(timezone.utc)
        diff_days = (run_date - birth_date).days
        self.age = round(diff_days / 365.25, 1)  # or use 365.25 for more precision

    def add_non_practice_trials(self, trial: LevanteTrial):
        self._non_practice_trials.append(trial)

    def check_non_practice_trials_count(self):
        trial_len_min = 10
        msg = []

        if len(self._non_practice_trials) < trial_len_min:
            self.validation_msg_run = f"less_than_{trial_len_min}_test_trials"

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
            if self.validation_msg_run:
                self.validation_msg_run = self.validation_msg_run + f"; straightlining_{consecutive_identical_min}"
            else:
                self.validation_msg_run = f"straightlining_{consecutive_identical_min}"

    def update_valid_run(self):
        self.valid_run = True if not self.validation_msg_run else False
        return self

    def validate_trials_in_run(self):
        self.check_non_practice_trials_count()
        self.check_straight_line_trials()
        self.update_valid_run()
        self.compute_below_chance_flags_scipy()


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
    validation_msg_user: Optional[str] = None

    _valid_group_ids: Set[str] = set()  # Private class attribute to hold valid group_ids
    _district_ids: Set[str] = set()

    @classmethod
    def set_valid_groups(cls, groups: List[GroupBase]):
        cls._valid_group_ids = {group.group_id for group in groups}

    @model_validator(mode='after')
    def check_birth_year_month(self):
        msg = []
        if self.user_type == 'student':
            if self.birth_year and self.birth_month and isinstance(self.birth_year, int) and isinstance(
                    self.birth_month, int):
                if self.birth_month not in range(1, 13):
                    self.birth_month = None
                    msg.append("birth_month_error")
                if self.birth_year < 2000:
                    self.birth_year = None
                    msg.append("birth_year_under_2000")
                if self.birth_year > 2050:
                    self.birth_year = None
                    msg.append("birth_year_greater_2050")

                    # Only compute age if valid year/month
                    if (
                            self.birth_year
                            and self.birth_month
                            and 1 <= self.birth_month <= 12
                            and 2000 <= self.birth_year <= 2050
                    ):
                        try:
                            # assume birth day = 15th
                            birth_date = datetime(
                                self.birth_year,
                                self.birth_month,
                                15,
                                tzinfo=timezone.utc
                            )
                            now_utc = datetime.now(timezone.utc)
                            age_days = (now_utc - birth_date).days
                            age_years = age_days / 365.25

                            if age_years > 18:
                                msg.append(f"user_over_18yo ({age_years:.1f} yrs)")
                        except Exception as e:
                            msg.append(f"birthdate_calc_error: {e}")
            else:
                self.birth_year = None
                self.birth_month = None
                msg.append("birth_year_month_missing")
        if msg:
            self.validation_msg_user = ";".join(msg)
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


class UserSchool(BaseModel):
    user_id: str
    school_id: str
    is_active: bool


class UserClass(BaseModel):
    user_id: str
    class_id: str
    is_active: bool


class UserGroup(BaseModel):
    user_id: str
    group_id: str
    is_active: bool


class AdministrationTask(BaseModel):
    administration_id: str
    task_id: str
    variant_id: Optional[str] = None


class UserAssignment(BaseModel):
    user_id: str
    assignment_id: str
    started: bool
    date_time: datetime
