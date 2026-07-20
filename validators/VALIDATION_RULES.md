# Validation Rules

> **Maintenance:** Keep this file in sync with validation logic in
> `validators/core_models.py`, `validators/entity_controller.py`, and
> `shared/firestore_services.py`. Update it whenever validation rules, message
> labels, or field names change.

This document describes the validation conditions, rules, and logic applied to
**users**, **runs**, **trials**, **variants**, **surveys**, and
**survey_responses** in the LEVANTE data-validator pipeline.

Implementation lives primarily in:

- `validators/core_models.py` — Pydantic models and field/model validators
- `validators/entity_controller.py` — orchestration, filtering, and run/trial aggregation
- `shared/firestore_services.py` — Firestore ingest and survey document parsing

---

## How validation works

### Two outcomes per row

| Outcome | Where it goes | Meaning |
|--------|----------------|---------|
| **Schema failure** | `invalid_data` table (via `invalid_*` lists) | Required fields missing, wrong types, or other Pydantic parse errors. Row is not exported in the main table. |
| **Soft validation** | Main table with `valid_* = false` and `validation_msg_*` | Row parses successfully but fails business rules. Value is kept for review. |

### Warnings vs validation failures

Some checks populate `warning_msg_*` instead of `validation_msg_*`:

- **Trials:** `warning_msg_trial` (e.g. missing/non-integer `trial_index`) does **not** set `valid_trial = false`.
- **Runs:** `warning_msg_run` is reserved; run validity is driven only by `validation_msg_run`.

### Pipeline order

For each org in a request:

1. Fetch and validate **users**
2. If not guest mode: fetch **surveys** and **survey_responses**
3. Fetch **runs**, then **trials** (trials drive run-level checks)
4. Fetch **tasks** and **variants** (schema only; no run/trial business rules)

---

## Users (`LevanteUser`)

**Model:** `LevanteUser` in `core_models.py`  
**Controller:** `EntityController.set_users()` / `process_users()`

### Required schema fields

| Field | Type | Notes |
|-------|------|-------|
| `user_id` | `str` | Required |
| `user_type` | `str` | Required (e.g. `student`, `teacher`, `parent`) |

Optional fields include `assessment_pid`, `email`, `birth_year`, `birth_month`, `sex`, `grade`, `parent1_id`, `parent2_id`, `teacher_id`, timestamps, etc.

### Ingest logic

- Users without `user_id` / `uid` are skipped.
- Duplicate `user_id` values are skipped (first occurrence wins).
- Schema parse failures are recorded in `invalid_users`.

Firestore inclusion (non-guest), before sampling:

| `user_type` | Required activity in `date_filter` window |
|-------------|-------------------------------------------|
| `student` | Assignment in range **and** ≥1 run (`timeStarted`) |
| `teacher` | Assignment in range **and** ≥1 survey response (`createdAt` or `timeStarted`) |
| `parent` | Assignment in range **and** ≥1 survey response (when not using stratified sample) |
| `admin` | Excluded |

Guests: must have ≥1 run (any time); `user_number_limit` uses random sampling without stratification.

### `user_number_limit` (stratified sample)

When `user_number_limit` is set on an org (non-guest), users are **not** chosen uniformly at random. The limit is a **total headcount** split as:

| Slot | Share of limit | Rule |
|------|----------------|------|
| Students | 40% (`int(limit × 0.4)`) | Random sample from eligible students (assignment + run in range) |
| Parents | 40% (`int(limit × 0.4)`) | Random sample from `parent1_id` / `parent2_id` of selected students with a survey in range |
| Teachers | Remainder (`limit − student − parent quotas`) | Random sample from eligible teachers (assignment + survey in range) |

Example: `user_number_limit: 20` → **8** students + **8** parents + **4** teachers = **20 total**.

If a bucket has fewer eligible users than its quota, all eligible users in that bucket are taken and a warning is logged (total may be below the limit).

Without `user_number_limit`, all users passing the filters above are included (plus one-hop relationship backfill for linked parents/teachers/children).

### Business rules (students only)

When `user_type == 'student'`, birth date checks run in `check_birth_year_month`:

| Condition | `validation_msg_user` |
|-----------|------------------------|
| `birth_month` missing or not an `int` | `birth_month_missing` |
| `birth_month` not in 1–12 | `birth_month_error(<value>)` |
| `birth_year` missing or not an `int` | `birth_year_missing` |
| `birth_year` < 2000 | `birth_year_under_2000(<value>)` |
| `birth_year` > current UTC year | `birth_year_greater_current_year(<value>)` |
| Computed age < 2 years | `user_under_2yo (<years> yrs)` |
| Computed age > 18 years | `user_over_18yo (<years> yrs)` |
| Date math error | `birthdate_calc_error: <error>` |

Age is computed assuming birth on the **15th** of the birth month (UTC).

Non-student user types do not receive birth-date business validation.

### Validity flag

```text
valid_user = True  iff  validation_msg_user is empty
```

Birth-year/month are **not** stripped or corrected; offending values stay on the row and appear in the message.

---

## Runs (`LevanteRun`)

**Model:** `LevanteRun` in `core_models.py`  
**Controller:** `EntityController.set_runs()` → `process_trials()` → `run.validate_trials_in_run()`

### Required schema fields

| Field | Type | Notes |
|-------|------|-------|
| `run_id` | `str` | Required |
| `user_id` | `str` | Required |
| `task_id` | `str` | Required |
| `variant_id` | `str` | Required |

### Ingest logic

- Runs with `task_id == "intro"` are **dropped** (not stored).
- Schema parse failures go to `invalid_runs`.
- `age` is computed when the linked user has valid `birth_year`, `birth_month` (1–12, year 1–9999), and the run has `time_started`. Uses mid-month birth assumption (15th, UTC), rounded to one decimal year.

### Run-level business rules (after trials are attached)

Non-practice **test trials** are collected during trial ingest (see Trials). Then:

#### 1. Minimum test trial count — `check_non_practice_trials_count`

| Condition | Message |
|-----------|---------|
| Fewer than **10** non-practice test trials | `less_than_10_test_trials` |

#### 2. Response straight-lining — `check_straight_line_trials`

- Non-practice trials sorted by integer `trial_index` (invalid indices sort last).
- Uses `response_location` values only for trials with integer `trial_index`.
- **10 or more consecutive identical** non-empty `response_location` values → `straightlining_10` (appended with `;` if other messages exist).

#### 3. Below-chance performance — `compute_below_chance_flags_scipy`

- Computes one-tailed binomial p-value `bc_p_below` (4AFC, chance p = 0.25).
- Requires at least **8** non-practice trials with a `correct` field; otherwise `bc_p_below` stays `None`.
- Does **not** currently set `validation_msg_run` (informational column only).

### Validity flag

```text
valid_run = True  iff  validation_msg_run is empty
```
### Warnings (do not fail the run)

| Condition | `warning_msg_run` |
|-----------|-------------------|
| `stop_type` present but not `taskAbort`, `timeOut`, `errorOut`, `sufficientTrials`, or `earlyCompletion`  | `stop_type_invalid(<value>)` |

---

## Trials (`LevanteTrial`)

**Model:** `LevanteTrial` in `core_models.py`  
**Controller:** `EntityController.set_trials()`

### Required schema fields

| Field | Type | Notes |
|-------|------|-------|
| `trial_id` | `str` | Required |
| `user_id` | `str` | Required |
| `run_id` | `str` | Required |
| `task_id` | `str` | Required |
| `assessment_stage` | `str` | Required |

### Ingest filtering (trials not stored)

Trials are skipped entirely when any of the following hold:

- `"instruction"` in `assessment_stage` (case-insensitive)
- `"display"` in `trial_mode` (case-insensitive)
- `"training"` in `corpus_trial_type` (case-insensitive)

### Practice vs test trials

`is_practice_trial` is set to `True` when:

- `"practice"` or `"training"` appears in `assessment_stage`, or
- `"practice"` appears in `corpus_trial_type`

A trial counts as a **non-practice test trial** for run checks when:

- `assessment_stage == 'test_response'`, **or**
- `is_practice_trial` is explicitly `False`

### Pre-processing

| Rule | Behavior |
|------|----------|
| `response` normalization | String `"nan"` / `"NaN"` and float `NaN` → `None` |
| `rt_numeric` | Derived from `rt` by truncating to int; empty/`0`/`"{}"` → `None` |

### Reaction time (`rt`) — non-practice trials only

Default bounds: **100 ms – 10,000 ms**.

Task-specific overrides:

| `task_id` | Min RT | Max RT |
|-----------|--------|--------|
| `matrix-reasoning` | 300 ms | 60,000 ms |
| `egma-math` | 100 ms | 60,000 ms |

| Condition | `validation_msg_trial` |
|-----------|------------------------|
| `rt` is `None`, `""`, `"{}"`, or `0` | `rt_missing` |
| Integer `rt` below min | `fast_rt_<min_seconds>s` (e.g. `fast_rt_0.1s`) |
| Integer `rt` above max | `slow_rt_<max_seconds>s` (e.g. `slow_rt_10.0s`) |
| String `rt` parsed as dict: any value ≤ min | `fast_rt_<min_seconds>s` |
| String `rt` parsed as dict: any value ≥ max | `slow_rt_<max_seconds>s` |
| String `rt` fails `ast.literal_eval` | `rt string converted to dict failed as <error>` |

Practice trials skip RT validation.

### Warnings (do not fail the trial)

| Condition | `warning_msg_trial` |
|-----------|---------------------|
| `trial_index` missing | `trial_index_missing` |
| `trial_index` present but not `int` | `trial_index_not_int` |
| `input_type` present but not `touch` or `mouse/keyboard` (case-insensitive) | `input_type_invalid` |

### Validity flag

```text
valid_trial = True  iff  validation_msg_trial is empty
```

---

## Tasks (`TaskBase`)

**Model:** `TaskBase` in `core_models.py`  
**Controller:** `EntityController.set_tasks()`

### Required schema fields

| Field | Type | Notes |
|-------|------|-------|
| `task_id` | `str` | Required |
| `task_name` | `str` | Required |
| `created_at` | `datetime` | Required; Firestore `createdAt` |
| `updated_at` | `datetime` | Required; Firestore `lastUpdated` → `updated_at` |

---

## Administrations (`AdministrationBase`)

**Model:** `AdministrationBase` in `core_models.py`  
**Controller:** `EntityController.set_administrations()`

### Required schema fields

| Field | Type | Notes |
|-------|------|-------|
| `administration_id` | `str` | Required |
| `administration_name` | `str` | Required |
| `created_at` | `datetime` | Required; Firestore `createdAt` |
| `updated_at` | `datetime` | Required; Firestore `updatedAt` |
| `date_created` | `datetime` | Required; Firestore `dateCreated` |
| `date_opened` | `datetime` | Required |
| `date_closed` | `datetime` | Required |

---

## Variants (`VariantBase`)

**Model:** `VariantBase` in `core_models.py`  
**Controller:** `EntityController.set_variants()` (one variant list per valid task)

### Required schema fields

| Field | Type | Notes |
|-------|------|-------|
| `variant_id` | `str` | Required |
| `task_id` | `str` | Required |
| `created_at` | `datetime` | Required; Firestore `createdAt` |
| `updated_at` | `datetime` | Required; Firestore `lastUpdated` or `updatedAt` → `updated_at` |

### Transformations (not failures)

| Field | Rule |
|-------|------|
| `language` | `en` → `en-US`, `es` → `es-CO`, `de` → `de-DE`; regional tags (e.g. `es-AR`, `en-us`) → `language-REGION` with region uppercased (`es-AR`, `en-US`); other non-empty values kept as trimmed text (no `Other(...)` wrapper); blank/`None` → `None` |
| `cat` | From Firestore `params.cat` (optional); used for adaptive detection |
| `adaptive` | `True` if `cat` is `true`; `False` if `cat` is `false`; if `cat` is absent, `True` when `variant_name` contains `"adaptive"` (case-insensitive), else `False` |

### Business validation

| Condition | `validation_msg_variant` | `valid_variant` |
|-----------|------------------------|-----------------|
| `language` present but not `xx-XX` after normalization (e.g. bare `fr`, `custom`) | `invalid_language_format(<language>)` | `false` |
| `language` is `None` / blank | _(empty)_ | `true` |
| `language` normalizes to `xx-XX` (e.g. `es-AR`, `en-US`) | _(empty)_ | `true` |

```text
valid_variant = True  iff  validation_msg_variant is empty
```

Pydantic schema parse failures still go to `invalid_variants` / `invalid_data` only.

---

## Surveys (`Survey`)

**Model:** `Survey` in `core_models.py`  
**Controller:** `EntityController.set_surveys()`  
**Ingest:** `FirestoreServices.get_surveys()` and `_parse_run_like_survey()`

### Required schema fields

| Field | Type | Notes |
|-------|------|-------|
| `survey_id` | `str` | Join key to survey_responses |
| `user_id` | `str` | Required |
| `survey_type` | `str` | `caregiver`, `student`, or `teacher` (from user type: `parent` → `caregiver`) |
| `created_at` | `datetime` | Required |
| `updated_at` | `datetime` | Optional; sourced from Firestore `updatedAt` (legacy docs may omit it) |

`survey_id` format:

- General: `{user_id}:{assignment_id}:general`
- Specific: `{user_id}:{assignment_id}:specific` or `...:specific:{specific_scope_id}`
- Duplicate losers (export only): append `:dup1`, `:dup2`, … to the logical id above

### Firestore document shapes

| Schema label | Description |
|--------------|-------------|
| `legacy_data` | `data.surveyResponses` flat map |
| `legacy_general_specific` | `general` and/or `specific[]` sections |
| `task_run` | Run-like doc with `taskId`, `timeStarted`; answers in `trials` subcollection |
| `pageNo_marker` | Draft/autosave row — **skipped** |
| `unknown` | Unrecognized shape — **skipped** (logged) |

### Date-range filtering (per section)

After a Firestore doc is fetched, each **general**, **specific**, **legacy data**, or **run-like** section is exported only if it has at least one response whose computed `timestamp` is within the request `start_date`–`end_date` window. Sections with no in-range answers are omitted entirely (no orphan `Survey` row). Within a kept section, only in-range responses are exported.

Example: a doc with `general` answered in 2025 and `specific[0]` answered in 2026 is exported for `2024-01-01`–`2025-12-31` as the general survey only.

### Duplicate Firestore documents

When multiple `surveyResponses` Firestore docs resolve to the same logical survey
(same `administration_id`, `user_id`, `survey_part`, `survey_type`,
`specific_scope_id`, `specific_scope`, and `survey_schema_source`), duplicates
are resolved as follows. Tie-breaking uses **`updated_at`**, falling back to
`created_at` when `updatedAt` is missing on the Firestore doc.

| Scenario | Valid row | Invalid rows |
|----------|-----------|--------------|
| Exactly one `is_complete == true` | That completed row | Others → `Duplicate` |
| Two or more `is_complete == true` | None (all flagged) | Each completed → `multiple_completed_surveys`; each incomplete → `Duplicate` |
| None complete | Latest by `updated_at` (fallback `created_at`) | Others → `Duplicate` |

- Superseded rows: `validation_msg_survey = "Duplicate"`, `valid_survey = false`.
- Ambiguous multiple-completed rows: `validation_msg_survey = "multiple_completed_surveys"`.
- Matching `survey_responses` rows inherit the same message and skip further validation.

**Unique `survey_id` per Firestore doc:** Losers get `:dup1`, `:dup2`, … appended to
the logical `survey_id` (oldest loser by `updated_at`, else `created_at`, is `:dup1`).
The winner keeps the unsuffixed id. When two or more rows are completed, there is no
winner — every row in the group is suffixed (`:dup1`, `:dup2`, …). Child
`survey_responses` use the same suffixed `survey_id` as their source doc.

`survey_schema_source` is part of the dedup key so legacy and run-like shapes
are never merged even if they share an assignment id.

This replaces the old `split_survey_assignment(...)` flag that marked every
duplicate invalid.

### Other survey-level messages (`validation_msg_survey`)

Set during Firestore parsing (before or at model construction):

| Message | When |
|---------|------|
| `Duplicate` | Superseded duplicate Firestore doc (see above) |
| `multiple_completed_surveys` | Two or more completed docs share the same dedup key |
| `specific_survey_multiple_scope_ids(...)` | Specific section has more than one of `childId`, `classId`, `schoolId` |
| `specific_survey_missing_scope` | Specific section with no resolved scope field |
| `specific_survey_missing_scope_id` | Scope field present but id value missing |
| `class_id_not_on_user(<class_id>)` | Specific scope is `class_id` but id not in user's active classes |
| `unexpected_run_like_task_id('<taskId>')` | Run-like doc where `taskId != 'child-survey'` |
| `survey_type_mismatch(task_id=child-survey,expected=student,user_type=...)` | Run-like child survey but user type does not normalize to `student` |

### Validity flag

```text
valid_survey = True  iff  validation_msg_survey is empty
```

If `validation_msg_survey` is pre-set during ingest, the model preserves it via `_derive_valid_survey`.

Guest orgs skip survey processing entirely.

---

## Survey responses (`SurveyResponse`)

**Model:** `SurveyResponse` in `core_models.py`  
**Controller:** `EntityController.set_survey_responses()`  
**Question catalog:** `get_survey_questions()` in `core_models.py`

### Required schema fields

| Field | Type | Notes |
|-------|------|-------|
| `survey_id` | `str` | FK to surveys |
| `question` | `str` | Question variable id |
| `timestamp` | `datetime` | Required |

Response value is stored in exactly one of: `boolean_response`, `string_response`, `numeric_response` (via `coerce_response_from_raw`).

### Question catalog

Questions are loaded from Redivis `levante-metadata-items.survey_items` (cached, TTL default 300s). On failure, falls back to local `survey_questions.json` next to `core_models.py`.

Each catalog entry includes `survey_section`, `question_survey_type`, `response_type`, and `response_options`.

### Ingest logic (`get_surveys`)

- Firestore docs are queried by top-level `createdAt` or `timeStarted`, then **each survey section** (general, specific, legacy `data`, run-like trials) is kept only when it has at least one response whose `timestamp` falls in the request date range.
- Per-response `timestamp`: `responseTime` when present on the answer object, otherwise the parent survey doc's `createdAt` (legacy) or trial `serverTimestamp` / `createdAt` / `timeStarted` (run-like).
- Intro question keys (containing `"intro"`) are skipped when flattening legacy responses.
- Nested dict/list response shapes are flattened or JSON-stringified as needed.
- Run-like (`task_run`) responses use trial `audioFile` as `question` and `responseLocation` as numeric response; practice and instruction trials are skipped.

### Validation steps (in order)

#### 0. Duplicate suppression

Rows from superseded or ambiguous Firestore docs are marked before model validation:

| Condition | `validation_msg_survey_response` |
|-----------|-----------------------------------|
| Response belongs to a superseded duplicate survey doc | `Duplicate` |
| Response belongs to a doc in a multiple-completed group | `multiple_completed_surveys` |

These rows skip coercion and schema checks so the preset message is preserved.

#### 1. Question existence — auto model validator

| Condition | `validation_msg_survey_response` |
|-----------|-----------------------------------|
| Empty/whitespace `question` | `question_empty` |
| `question` not in catalog | `question_not_in_survey_questions(<question>)` |

#### 2. Response coercion — `coerce_response_from_raw`

Uses `response_type` hint when provided (`boolean`, `numeric`, etc.); otherwise infers bool → numeric → string.

Does not append validation messages on its own.

#### 3. Schema alignment — `validate_response_against_schema`

Compares row to catalog metadata for the question:

| Condition | `validation_msg_survey_response` |
|-----------|-----------------------------------|
| Catalog section ≠ row `survey_part` | `survey_section_mismatch(expected:<section>, got:<part>)` |
| Catalog survey type ≠ row survey type (after normalizing `parent`→`caregiver`, `student`→`child`) | `question_survey_type_mismatch(expected:<type>, got:<type>)` |

**Response type vs allowed options** checks are intentionally **paused** (comment in code); section and survey-type checks remain active.

### Validity flag

```text
valid_survey_response = True  iff  validation_msg_survey_response is empty after all steps
```

`survey_schema_source` (`legacy_data`, `legacy_general_specific`, `task_run`) is carried for observability but does not change message labels.

---

## Quick reference: validation message fields

| Entity | Valid flag | Message field | Warning field |
|--------|------------|---------------|---------------|
| User | `valid_user` | `validation_msg_user` | — |
| Run | `valid_run` | `validation_msg_run` | `warning_msg_run` |
| Trial | `valid_trial` | `validation_msg_trial` | `warning_msg_trial` |
| Variant | — (schema only) | — | — |
| Survey | `valid_survey` | `validation_msg_survey` | — |
| Survey response | `valid_survey_response` | `validation_msg_survey_response` | — |

Multiple messages on one row are joined with `;`.
