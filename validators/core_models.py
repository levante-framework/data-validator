from pydantic import BaseModel, Extra, Field, field_validator, model_validator, ValidationError
from typing import Optional, Union, List, Set, Any, Literal, get_origin, get_args
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import time
import logging
import ast
import re
from scipy.stats import binom, binomtest
from math import isnan


_SURVEY_Q_CACHE: Optional[dict] = None
_SURVEY_Q_CACHE_TS: float = 0.0


def _load_survey_questions_from_local_json() -> dict:
    path = Path(__file__).with_name("survey_questions.json")
    with path.open("r", encoding="utf-8") as f:
        questions = json.load(f)
    if not isinstance(questions, dict):
        raise ValueError("survey_questions.json must contain a top-level object")
    return questions


def _load_survey_questions_from_redivis() -> dict:
    import redivis
    import settings
    from shared.secret_services import secret_service

    if not os.getenv("REDIVIS_API_TOKEN"):
        os.environ["REDIVIS_API_TOKEN"] = secret_service.get_secret_payload(
            secret_id=settings.config["REDIVIS_API_TOKEN_SECRET_ID"],
            version_id="latest",
        )
    if not os.getenv("REDIVIS_IDENTITY"):
        os.environ["REDIVIS_IDENTITY"] = secret_service.get_secret_payload(
            secret_id=settings.config["REDIVIS_IDENTITY_ACCOUNT_SECRET_ID"],
            version_id="latest",
        )

    # Metadata dataset/table source of truth
    org = redivis.organization(settings.config["INSTANCE"])
    ds = org.dataset(name="levante-metadata-items")
    table = ds.table("survey_items")
    rows = table.to_pandas_dataframe().to_dict(orient="records")

    questions: dict[str, dict] = {}
    for row in rows:
        variable = row.get("variable")
        if not variable:
            continue
        questions[str(variable)] = {
            "survey_section": row.get("survey_part"),
            "question_survey_type": row.get("survey_type"),
            "response_type": row.get("response_type"),
            "response_options": row.get("values"),
        }
    if not questions:
        raise ValueError("No survey items loaded from Redivis survey_items table")
    return questions


def get_survey_questions() -> dict:
    global _SURVEY_Q_CACHE, _SURVEY_Q_CACHE_TS

    ttl_seconds = int(os.getenv("SURVEY_QUESTIONS_TTL_SECONDS", "300"))
    now = time.time()
    if _SURVEY_Q_CACHE is not None and now - _SURVEY_Q_CACHE_TS < ttl_seconds:
        return _SURVEY_Q_CACHE

    try:
        questions = _load_survey_questions_from_redivis()
        logging.info(f"Loaded survey questions from Redivis survey_items table. count={len(questions)}")
    except Exception as e:
        logging.warning(f"Failed loading survey questions from Redivis, using local JSON fallback: {e}")
        questions = _load_survey_questions_from_local_json()
        logging.info(f"Loaded survey questions from local survey_questions.json fallback. count={len(questions)}")

    _SURVEY_Q_CACHE = questions
    _SURVEY_Q_CACHE_TS = now
    return questions


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
    subtask: Optional[str] = None
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

        if self.is_practice_trial is not True:
            if self.rt not in [None, "", "{}", "0", 0]:
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
        if self.trial_index is not None:
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
        # Guard against out-of-range / nonsensical birth values (e.g. year 10000,
        # month 13, etc.) so a single bad user doc can't crash the whole run.
        if not (
            isinstance(birth_year, int)
            and isinstance(birth_month, int)
            and 1 <= birth_month <= 12
            and 1 <= birth_year <= 9999
        ):
            return None
        try:
            birth_date = datetime(
                birth_year,
                birth_month,
                15,  # assume middle of the month
                tzinfo=timezone.utc
            )
        except (ValueError, OverflowError):
            return None
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
            # Check if the list has fewer than n elements or all elements are blank
            if len(lst) < n or all(x in ("", None) for x in lst):
                return False

            # Loop through the list, stopping at the nth-to-last element
            for i in range(len(lst) - n + 1):
                # Check if the n elements starting from index i are all the same
                if all(lst[i] == lst[j] and lst[j] not in ("", None) for j in range(i, i + n)):
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
    birth_year: Optional[int] = None  # Field(None, ge=2000, le=current_year)
    birth_month: Optional[int] = None  # Field(None, ge=1, le=12)
    sex: Optional[str] = None
    grade: Optional[int] = None

    valid_user: Optional[bool] = None
    validation_msg_user: Optional[str] = None

    @model_validator(mode='after')
    def check_birth_year_month(self):
        msg = []
        current_year = datetime.now(timezone.utc).year
        if self.user_type == 'student':
            # Keep numeric values as-is; only flag validation messages. The actual
            # offending value is included in the message so it's visible in the
            # output table without cross-referencing the row.
            if isinstance(self.birth_month, int):
                if self.birth_month not in range(1, 13):
                    msg.append(f"birth_month_error({self.birth_month})")
            else:
                msg.append("birth_month_missing")

            if isinstance(self.birth_year, int):
                if self.birth_year < 2000:
                    msg.append(f"birth_year_under_2000({self.birth_year})")
                elif self.birth_year > current_year:
                    msg.append(f"birth_year_greater_current_year({self.birth_year})")
            else:
                msg.append("birth_year_missing")

            # Only compute age if valid year/month
            if (
                    isinstance(self.birth_year, int)
                    and isinstance(self.birth_month, int)
                    and 1 <= self.birth_month <= 12
                    and 2000 <= self.birth_year <= current_year
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

                    if age_years < 2:
                        msg.append(f"user_under_2yo ({age_years:.1f} yrs)")
                    if age_years > 18:
                        msg.append(f"user_over_18yo ({age_years:.1f} yrs)")
                except Exception as e:
                    msg.append(f"birthdate_calc_error: {e}")
        if msg:
            self.validation_msg_user = ";".join(msg)
        return self

    @model_validator(mode='after')
    def update_valid_user(self):
        self.valid_user = True if not self.validation_msg_user else False
        return self


class Survey(BaseModel):
    survey_id: str  # {user_id}:{administration_id}:{survey_part}[:{specific_scope_id}]
    administration_id: Optional[str] = None
    user_id: str
    survey_part: Optional[str] = None
    # Redivis scope label (child_id / class_id / school_id); sourced from Firestore childId etc.
    specific_scope: Optional[str] = None
    # Value of that field (child user id, class id, school id, …).
    specific_scope_id: Optional[str] = None
    survey_type: str  # caregiver, student, teacher
    is_complete: Optional[bool] = None
    created_at: datetime
    updated_at: Optional[datetime] = None  # Firestore updatedAt; optional on legacy docs
    # New columns for the unified surveyResponses parser (legacy + task_run).
    # survey_schema_source labels which Firestore document shape this row came
    # from so analysts can spot schema rollouts in Redivis.
    survey_schema_source: Optional[str] = None
    valid_survey: Optional[bool] = None
    validation_msg_survey: Optional[str] = None

    @model_validator(mode='after')
    def _derive_valid_survey(self):
        if self.valid_survey is None:
            self.valid_survey = self.validation_msg_survey in (None, "")
        return self


class SurveyResponse(BaseModel):
    survey_id: str  # join key to surveys.survey_id ({user_id}:{assignment_id}:...)
    question: str

    boolean_response: Optional[bool] = None
    string_response: Optional[str] = None
    numeric_response: Optional[float] = None

    timestamp: datetime
    # Source schema this response was extracted from (legacy_data /
    # legacy_general_specific / task_run). Carried through from
    # firestore_services.get_surveys for observability — validation
    # messages remain uniform across sources.
    survey_schema_source: Optional[str] = None
    valid_survey_response: Optional[bool] = None
    validation_msg_survey_response: Optional[str] = None

    @model_validator(mode='after')
    def _check_question_in_survey_questions(self):
        """
        Auto-run at construction: every `question` must exist in
        get_survey_questions() (loaded from Redivis survey_items, with a
        local JSON fallback). One uniform message label regardless of
        source — the offending question id is included so it's trivial to
        identify and the source can be cross-referenced via
        `survey_schema_source` on this row.
        """
        q = (self.question or "").strip() if isinstance(self.question, str) else ""
        if not q:
            # Required field; pydantic already rejects None at parse time. An
            # empty/whitespace question gets a dedicated message.
            self._append_validation_msg_survey_response("question_empty")
            self.valid_survey_response = False
            return self

        if q not in get_survey_questions():
            self._append_validation_msg_survey_response(f"question_not_in_survey_questions({q})")
            self.valid_survey_response = False
        return self

    def _append_validation_msg_survey_response(self, m: str) -> None:
        """Append a message to validation_msg_survey_response, preserving any earlier entries."""
        if not m:
            return
        if self.validation_msg_survey_response:
            # Avoid trivial duplicates from the same validator running twice.
            parts = self.validation_msg_survey_response.split(";")
            if m in parts:
                return
            self.validation_msg_survey_response = f"{self.validation_msg_survey_response};{m}"
        else:
            self.validation_msg_survey_response = m

    def coerce_response_from_raw(self, response: Any = None, response_type: Optional[str] = None):
        self.boolean_response = None
        self.numeric_response = None
        self.string_response = None

        if response is None:
            return self

        hint = str(response_type or "").strip().lower()

        def parse_bool(v: Any) -> Optional[bool]:
            if isinstance(v, bool):
                return v
            if isinstance(v, str):
                low = v.strip().lower()
                if low in {"yes", "true"}:
                    return True
                if low in {"no", "false"}:
                    return False
            return None

        def parse_num(v: Any) -> Optional[float]:
            if isinstance(v, bool):
                return None
            if isinstance(v, (int, float)):
                return round(float(v), 3)
            if isinstance(v, str):
                s = v.strip()
                if s == "":
                    return None
                try:
                    return round(float(s), 3)
                except Exception:
                    return None
            return None

        if hint in {"boolean", "bool"}:
            b = parse_bool(response)
            if b is not None:
                self.boolean_response = b
            else:
                self.string_response = str(response)
            return self

        if hint in {"numeric", "number", "float", "int", "integer"}:
            n = parse_num(response)
            if n is not None:
                self.numeric_response = n
            else:
                self.string_response = str(response)
            return self

        # Generic fallback if no reliable hint.
        b = parse_bool(response)
        if b is not None:
            self.boolean_response = b
            return self
        n = parse_num(response)
        if n is not None:
            self.numeric_response = n
            return self
        self.string_response = str(response)
        return self

    def validate_response_against_schema(
        self,
        survey_part: Optional[str] = None,
        survey_type: Optional[str] = None,
    ):
        """
        Section / survey-type checks against the questions metadata.

        Question existence (`question in get_survey_questions()`) is handled by
        the auto-running model validator `_check_question_in_survey_questions`
        — this method intentionally does NOT re-check it. Messages from this
        method are appended to any existing validation_msg_survey_response rather than
        overwriting it.
        """

        def normalize_survey_type(v: Optional[str]) -> Optional[str]:
            if v is None:
                return None
            v_norm = str(v).strip().lower()
            # Normalize synonyms used across data sources/questions dictionary
            if v_norm == "parent":
                return "caregiver"
            if v_norm == "student":
                return "child"
            return v_norm

        question_meta = get_survey_questions().get(self.question)
        if question_meta:
            expected_section = question_meta.get("survey_section")
            if expected_section and survey_part:
                if str(expected_section).lower() != str(survey_part).lower():
                    self._append_validation_msg_survey_response(
                        f"survey_section_mismatch(expected:{expected_section}, got:{survey_part})"
                    )

            expected_type = normalize_survey_type(survey_type)
            actual_type = normalize_survey_type(question_meta.get("question_survey_type"))
            if expected_type and actual_type and expected_type != actual_type:
                self._append_validation_msg_survey_response(
                    f"question_survey_type_mismatch(expected:{expected_type}, got:{actual_type})"
                )

            # Temporarily pause strict response type checks.
            # Keep question existence / section / survey-type validations active.

        # valid_survey_response stays False if any prior validator (including
        # _check_question_in_survey_questions) already failed; otherwise we
        # confirm it as True only when no messages were accumulated.
        if self.validation_msg_survey_response:
            self.valid_survey_response = False
        else:
            self.valid_survey_response = True
        return self


class SiteBase(BaseModel):
    site_id: str
    site_abbreviation: Optional[str] = None
    site_name: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class CohortBase(BaseModel):
    cohort_id: str
    cohort_name: str
    cohort_abbreviation: Optional[str] = None
    tags: Optional[str] = None
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None

    @field_validator("tags", mode="before")
    def normalize_response_nan(cls, v):
        """
        Convert 'nan' strings or float('nan') to None (Firestore NULL).
        """
        if v is None:
            return None
        else:
            return str(v)


class SchoolBase(BaseModel):
    school_id: str
    district_id: str
    school_name: str
    school_abbreviation: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ClassBase(BaseModel):
    class_id: str
    school_id: str
    district_id: str
    class_name: str
    class_abbreviation: Optional[str] = None
    grade: Optional[Union[str, int]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class TaskBase(BaseModel):
    task_id: str
    task_name: str
    description: Optional[str] = None
    last_updated: datetime


class VariantBase(BaseModel):
    variant_id: str
    task_id: str
    variant_name: Optional[str] = None
    button_layout: Optional[str] = None
    corpus: Optional[str] = None
    key_helpers: Optional[bool] = None
    language: Optional[str] = None
    cat: Optional[bool] = None
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
    valid_variant: Optional[bool] = None
    validation_msg_variant: Optional[str] = None

    _LANGUAGE_BCP47_PATTERN = re.compile(r"^[a-z]{2}-[A-Z]{2}$")

    @field_validator("language", mode="before")
    def normalize_language(cls, v):
        """
        Normalize language codes to BCP-47-style tags (language or language-REGION).
        Short codes: en -> en-US, es -> es-CO, de -> de-DE.
        Regional tags (e.g. es-AR, en-US) keep the language lowercased and region uppercased.
        Other non-empty values are returned trimmed, without an "Other(...)" wrapper.
        """
        if v is None:
            return None

        code = str(v).strip()
        if code == "":
            return None

        lower = code.lower().replace("_", "-")
        short_to_bcp47 = {
            "en": "en-US",
            "es": "es-CO",
            "de": "de-DE",
        }
        if lower in short_to_bcp47:
            return short_to_bcp47[lower]

        match = re.fullmatch(r"([a-z]{2})(?:-([a-z]{2}))?", lower)
        if match:
            lang, region = match.group(1), match.group(2)
            if region:
                return f"{lang}-{region.upper()}"
            return lang

        return code

    @model_validator(mode="after")
    def set_adaptive(self):
        """
        Adaptive when params.cat is true, else when variant_name contains 'adaptive'.
        If cat is false, not adaptive. If cat is missing, fall back to the name only.
        """
        if self.cat is True:
            self.adaptive = True
        elif self.cat is False:
            self.adaptive = False
        else:
            name = (self.variant_name or "").lower()
            self.adaptive = "adaptive" in name
        return self

    @model_validator(mode="after")
    def check_language_format(self):
        """Require normalized language to match xx-XX (e.g. en-US, es-AR) when present."""
        if self.language is None:
            return self
        if not self._LANGUAGE_BCP47_PATTERN.fullmatch(self.language):
            self.validation_msg_variant = (
                f"invalid_language_format({self.language})"
            )
        return self

    @model_validator(mode="after")
    def update_valid_variant(self):
        self.valid_variant = not self.validation_msg_variant
        return self


class AdministrationBase(BaseModel):
    administration_id: str
    administration_name: str
    public_name: Optional[str] = None
    sequential: bool
    created_by: str
    date_created: datetime
    date_closed: datetime
    date_opened: datetime


class UserAdministration(BaseModel):
    user_id: str
    administration_id: str
    date_assigned: Optional[datetime] = None
    date_started: Optional[datetime] = None
    is_completed: bool = False


class UserSite(BaseModel):
    user_id: str
    site_id: str
    is_active: bool


class UserCohort(BaseModel):
    user_id: str
    cohort_id: str
    is_active: bool


class UserSchool(BaseModel):
    user_id: str
    school_id: str
    is_active: bool


class UserClass(BaseModel):
    user_id: str
    class_id: str
    is_active: bool


def _schema_row_for_cls(cls, now: datetime | None = None):
    """
    Fabricate a one-row 'schema' sentinel instance for any Pydantic model.
    Rules (sane defaults):
      - str / Any -> "schema_row"
      - int -> 0
      - float -> 0.0001
      - bool -> False
      - datetime -> now (UTC)
      - list/set/tuple -> []
      - dict -> {}
      - otherwise -> declared default if present else "schema_row"
    """
    now = now or datetime.now(timezone.utc)
    vals: dict[str, Any] = {}
    for name, f in cls.model_fields.items():
        ann = f.annotation
        origin = get_origin(ann)
        base = ann

        # unwrap Optional/Union[..., None]
        if origin is Union:
            args = [a for a in get_args(ann) if a is not type(None)]
            base = args[0] if args else Any
            origin = get_origin(base)

        # choose sentinel
        if base in (str, Any):
            vals[name] = "schema_row"
        elif base is int:
            vals[name] = 0
        elif base is float:
            vals[name] = 0.0001
        elif base is bool:
            vals[name] = False
        elif base is datetime:
            vals[name] = now
        elif origin in (list, set, tuple):
            vals[name] = []
        elif origin is dict:
            vals[name] = {}
        else:
            vals[name] = f.default if f.default is not None else "schema_row"

    try:
        return cls(**vals)
    except Exception:
        # if validators complain, bypass validation (still fine for schema sentinel)
        return cls.model_construct(**vals)


# Attach schema_row to every BaseModel subclass defined in this module (once).
for _name, _obj in list(globals().items()):
    try:
        from pydantic import BaseModel as _BM  # local alias

        if isinstance(_obj, type) and issubclass(_obj, _BM) and _obj is not _BM:
            if not hasattr(_obj, "schema_row"):
                setattr(_obj, "schema_row", classmethod(_schema_row_for_cls))
    except Exception:
        pass
