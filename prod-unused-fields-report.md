# Levante `hs-levante-admin-prod`: data-validator Field Usage Report

This report covers the **`hs-levante-admin-prod`** Firestore (admin) database: how often each field the `data-validator` uses is actually populated, and which collections/fields the validator does **not** read or export.

**Method:** Live prod Firestore was queried directly. Field-population counts are exact full-database counts; the unused-collection/field lists were derived by sampling the schema and diffing against everything the validator reads (`firestore_services.py` queries + the Pydantic models in `core_models.py`). The validator ignores extra fields, so even within a queried document only the declared model fields are exported.

---

## Field population: how many prod documents contain each *used* field

Exact counts across the **entire** `hs-levante-admin-prod` database (field present and non-null). Subcollections (`runs`, `trials`, `surveyResponses`, `variants`) are counted across all parents via collection-group scans; the `trials` scan covered all 1,874,792 documents.

### Org / metadata collections

**districts** (total 79): `name` 79, `createdAt` 79, `updatedAt` 79, `abbreviation` 8

**groups** (total 251): `name` 251, `createdAt` 251, `tags` 155, `abbreviation` 93, **`lastUpdated` 0**

**schools** (total 125): `name` 125, `districtId` 125, `createdAt` 125, `updatedAt` 125, **`abbreviation` 1**

**classes** (total 196): `name` 196, `createdAt` 196, `updatedAt` 196, `districtId` 195, **`abbreviation` 1**

**administrations** (total 660): `name`, `publicName`, `sequential`, `createdBy`, `dateCreated`, `dateClosed`, `dateOpened` — all **660 (100%)**

**tasks** (total 27): `name` 25, `description` 25, `lastUpdated` 24

### Users / guests

**users** (total 17,639)

| field | count | field | count |
|---|---|---|---|
| userType | 17,633 | createdAt | 17,636 |
| email | 17,621 | lastUpdated | 17,523 |
| emailVerified | 17,377 | birthYear | 16,885 |
| birthMonth | 16,884 | groups | 16,858 |
| districts | 16,752 | grade | 16,672 |
| assignmentsAssigned | 15,859 | schools | 14,814 |
| classes | 14,814 | parentIds | 4,399 |
| assignmentsStarted | 3,137 | assignmentsCompleted | 2,942 |
| teacherIds | 2,445 | sex | 701 |
| assessmentPid | 7 | **created** | **0** |

**guests** (total 27,793): `userType` 27,793, `created` 27,793, `lastUpdated` 27,034, `assessmentPid` 14,404, `grade` 87 — and `birthYear` / `birthMonth` / `email` / `emailVerified` / `sex` = **0** (guests do not carry these).

### Subcollections (collection-group, whole DB)

**runs** (total 57,834 — users + guests combined)

| field | count | field | count |
|---|---|---|---|
| timeStarted | 57,834 | taskId | 57,834 |
| variantId | 57,834 | completed | 57,834 |
| reliable | 44,577 | assignmentId | 30,604 |
| bestRun | 30,604 | taskVersion | 30,433 |
| timeFinished | 29,926 | scores | 29,528 |

**surveyResponses** (total 3,842): `createdAt` 3,724, `general` 3,391, `updatedAt` 3,416, `administrationId` 3,416, `specific` 1,108, `data` 308, **`responses` 0**, **`isComplete` 0**

**variants** (total 1,250): `params` 1,250, `lastUpdated` 1,238, `name` 151

**trials** (total 1,874,792)

| field | count | % | field | count | % |
|---|---|---|---|---|---|
| time_elapsed | 1,874,792 | 100% | serverTimestamp | 1,874,792 | 100% |
| assessment_stage | 1,873,149 | 99.9% | correct | 1,855,988 | 99.0% |
| rt | 1,835,233 | 97.9% | isPracticeTrial | 1,471,916 | 78.5% |
| response | 1,413,779 | 75.4% | item | 1,364,569 | 72.8% |
| trialIndex | 1,337,770 | 71.4% | answer | 1,217,429 | 64.9% |
| distractors | 1,079,009 | 57.6% | responseSource | 1,068,334 | 57.0% |
| corpusTrialType | 1,063,744 | 56.7% | responseType | 948,687 | 50.6% |
| responseLocation | 915,647 | 48.8% | corpusId | 739,106 | 39.4% |
| itemUid | 559,084 | 29.8% | itemId | 529,978 | 28.3% |
| thetaEstimate | 514,091 | 27.4% | trial_index | 484,031 | 25.8% |
| response_source | 417,978 | 22.3% | difficulty | 242,075 | 12.9% |
| word | 242,075 | 12.9% | thetaEstimate2 | 242,175 | 12.9% |
| thetaSE | 242,175 | 12.9% | thetaSE2 | 242,175 | 12.9% |
| sequence | 101,712 | 5.4% | trialMode | 84,926 | 4.5% |

### Notable findings from the counts

- **`groups.lastUpdated` = 0** — `CohortBase` reads `last_updated`, but no group doc has it (groups use `updatedAt`, which the validator does not read). Cohort `last_updated` is always null.
- **`users.created` = 0** — `get_users` sets `created_at` from `created`, but real users have `createdAt` (covered by camelCase→snake_case normalization). The `created` lookup is effectively dead for users; only guests use `created`.
- **`surveyResponses.responses` and `isComplete` = 0** — the "newer root-level `{isComplete, responses}`" survey shape never occurs in prod; all real surveys use `general` / `specific` / `data`. That branch is currently dead.
- **`abbreviation`** is almost never set on schools/classes (1 each) and rare on districts (8), so `*_abbreviation` columns are mostly null.
- **camelCase vs snake_case in trials** — both `trialIndex` (1.34M) and `trial_index` (484k), and both `responseSource` (1.07M) and `response_source` (418k) appear, because different task builds emit different casing; the validator reads both.

---

## Collections in prod that the validator never reads

| Collection | Notes |
|---|---|
| `activationCodes` | org enrollment codes — never queried |
| `deleted-users` | soft-deleted users — never queried |
| `legal` | legal/consent docs — never queried |
| `packages` | task packages — never queried |
| `sites` | **never queried** — the validator's "site" data actually comes from the `districts` collection, not `sites` |
| `system` | system/version doc — never queried |
| `userClaims` | auth claims — never queried |
| `logs` | written to by the validator, never read |

---

## Unused fields within collections the validator *does* read

(Field names below are the source Firestore field names.)

### districts (read as "site")
Used: `name`, `abbreviation`, `createdAt`, `updatedAt`

Unused: `administrators`, `admins`, `archived`, `archivedClasses`, `archivedSchools`, `classes`, `createdBy`, `currentActivationCode`, `districtId`, `id`, `normalizedName`, `parentOrgId`, `schoolId`, `schools`, `siteId`, `subGroups`, `tags`, `type`, `validActivationCodes`

### groups (read as "cohort")
Used: `name`, `abbreviation`, `tags`, `createdAt`, `lastUpdated`

Unused: `address`, `archived`, `createdBy`, `currentActivationCode`, `demoData`, `districtId`, `id`, `ncesId`, `normalizedName`, `parentOrgId`, `parentOrgType`, `schoolId`, `siteId`, `testData`, `updatedAt`, `validActivationCodes`

### schools
Used: `name`, `abbreviation`, `districtId`, `createdAt`, `updatedAt`

Unused: `archived`, `classes`, `createdBy`, `currentActivationCode`, `id`, `normalizedName`, `parentOrgId`, `schoolId`, `siteId`, `tags`, `validActivationCodes`

### classes
Used: `name`, `abbreviation`, `districtId`, `createdAt`, `updatedAt`

Unused: `archived`, `createdBy`, `currentActivationCode`, `grade`, `id`, `normalizedName`, `parentOrgId`, `schoolId`, `siteId`, `tags`, `validActivationCodes`

### administrations
Used: `name`, `publicName`, `sequential`, `createdBy`, `dateCreated`, `dateClosed`, `dateOpened`

Unused: `assessments`, `classes`, `createdAt`, `creatorName`, `districts`, `families`, `groups`, `legal`, `minimalOrgs`, `normalizedName`, `readOrgs`, `schools`, `siteId`, `syncChunksCompleted`, `syncChunksTotal`, `syncStatus`, `tags`, `testData`, `updatedAt` (plus a stray `newField`)

### tasks
Used: `name`, `description`, `lastUpdated`

Unused: `createdAt`, `image`, `language`, `parent_survey`, `registered`, `runKeys`, `student_survey`, `taskURL`, `teacher_survey`, `trialKeys`, `tutorialVideo`, `updatedAt`

> Note: `runKeys` / `trialKeys` are only read by the schema-tracking code, which is currently commented out.

### tasks/variants
Used: `params` (spread into many config columns), `name`, `lastUpdated`

Unused: `blockTiming`, `createdAt`, `registered`, `taskURL`, `updatedAt`, `variantURL`

### users
Used: `userType`, `assignmentsAssigned`, `assignmentsStarted`, `assignmentsCompleted`, `birthYear`, `birthMonth`, `parentIds`, `teacherIds`, `grade`, `created`, `createdAt`, `lastUpdated`, `email`, `emailVerified`, `sex`, `assessmentPid`, `districts`, `groups`, `schools`, `classes`

Unused: `adminData`, `archived`, `assessmentUid`, `assignments`, `childIds`, `disabled`, `displayName`, `families`, `legal`, `name`, `roles`, `schoolLevel`, `sso`, `tasks`, `testData`, `uid`, `updatedAt`, `username`, `variants`

### guests
Used: `userType`, `assessmentPid`, `created`, `lastUpdated`, `birthYear`, `birthMonth`, `grade`, `email`, `emailVerified`, `sex`

Unused: `age`, `ageMonths`, `assessmentUid`, `classId`, `createdAt`, `districtId`, `language`, `schoolId`, `studyId`, `tasks`, `updatedAt`, `variants`

### users/runs & guests/runs
Used: `scores`, `timeStarted`, `timeFinished`, `taskId`, `taskVersion`, `assignmentId`, `variantId`, `reliable`, `completed`, `bestRun`

Unused: `assigningOrgs`, `cloudSyncTimestamp`, `createdAt`, `engagementFlags`, `id`, `readOrgs`, `reliableByBlock`, `updatedAt`, `userData`

### users/surveyResponses
Used: `createdAt`, `updatedAt`, `administrationId`, `data`, `general`, `specific`, `responses`, `isComplete`

Unused: `assigningOrgs`, `assignmentId`, `completed`, `id`, `pageNo`, `readOrgs`, `reliable`, `scores`, `taskId`, `taskVersion`, `timeFinished`, `timeStarted`, `userData`, `variantId`

> Note: the unused fields here look like run-document fields that leaked into a few survey docs.

### users/runs/trials
Used: `assessment_stage`, `trialIndex`/`trial_index`, `item`, `itemId`, `itemUid`, `answer` (falls back to `sequence`/`word`), `response`, `correct`, `difficulty`, `responseSource`/`response_source`, `time_elapsed`, `rt`, `serverTimestamp`, `isPracticeTrial`, `corpusId`, `corpusTrialType`, `responseType`, `responseLocation`, `distractors`, `trialMode`, `thetaEstimate`, `thetaEstimate2`, `thetaSE`, `thetaSE2`

Unused (35): `audioButtonPresses`, `audioFeedback`, `audioFile`, `block`, `blockId`, `blocks`, `button_response`, `corpus`, `correctResponse`, `createdAt`, `goal`, `incorrectPracticeResponses`, `internal_node_id`, `keyboard_response`, `realpseudo`, `responseInput`, `save_trial`, `selectedCoordinates`, `slider_start`, `start_time`, `start_time_unix`, `stim`, `stimulus`, `stimulusPresentationTime`, `stimulusRule`, `story`, `subtask`, `timezone`, `trialNumBlock`, `trialNumPractice`, `trialNumTotal`, `trial_type`, `truefalse`, `updatedAt`

---

## Caveats

- The "unused field" lists come from **sampling** (up to 400 docs/collection; fewer for deep subcollections like trials/surveys), so very rare fields may be missing, and some "unused" fields (e.g. run-like fields in `surveyResponses`, stray numeric keys in `deleted-users`) are clearly dirty/legacy data rather than real schema.
- The **field-population counts are exact** full-database counts (count of documents where the field is present and non-null). A handful of explicitly-null values would be excluded.
- "Used" is based on the Pydantic models, which ignore extra fields — so even within a queried doc, only declared model fields land in the output tables. (Trial docs are the exception in the ROAR branch via a `trial_attributes` blob, but `INSTANCE` is `LEVANTE` here, so that branch does not run.)
- This covers the **admin** database only. There is a separate `hs-levante-assessment-prod` Firestore that this validator does not read at all.
