# API payloads

HTTP trigger (`POST` to `data-validator-trigger` with `API-Key`) and Cloud Run Job
(`DATA_VALIDATOR_PAYLOAD`) use the same JSON body.

Omit `operation` to run **data validation**. Extra fields are rejected
(`extra=forbid` on data-validation payloads).

Boolean fields accept `true` / `false`, or strings `"true"` / `"1"` / `"yes"`
and `"false"` / `"0"` / `"no"`.

---

## `data_validation` (default)

Daily cron jobs use this shape. They omit `operation`, `skip_process_dataset`,
and `release_processed_dataset`.

```json
{
  "operation": "data_validation",
  "dataset_id": "pilot-uniandes-co-bogota-raw",
  "is_save_to_storage": true,
  "is_force_uploading_to_redivis": false,
  "skip_process_dataset": false,
  "release_processed_dataset": true,
  "send_slack": true,
  "orgs": [
    {
      "org_id": "pilot-uniandes-co-bogota-raw",
      "is_guest": false,
      "is_user_id_masked": false,
      "user_number_limit": null,
      "filters": {
        "org_filter": {
          "key": "districts",
          "operator": "array_contains_any",
          "value": ["kdCe535D1FGOtYp8YmYy"]
        },
        "date_filter": {
          "start_date": "2024-01-01",
          "end_date": "2025-12-31"
        },
        "user_filter": {
          "key": "assessmentPid",
          "operator": "starts_with",
          "value": "col"
        }
      }
    }
  ]
}
```

### Top-level fields

| Field | Required | Default | Meaning |
|-------|----------|---------|---------|
| `operation` | no | `data_validation` | Job type. |
| `dataset_id` | **yes** | — | Redivis / GCS dataset name (site crons use `{Name}-raw`). |
| `is_save_to_storage` | **yes** | — | `false`: validate only (no GCS / Redivis). `true`: write tables. |
| `is_force_uploading_to_redivis` | no | `false` | `true`: force a new raw Redivis version even if GCS is unchanged. |
| `skip_process_dataset` | no | `false` | `true`: after a new raw release, do **not** run `process_dataset`. Omit / `false` / `null`: run the notebook. **Cron jobs need no change.** |
| `release_processed_dataset` | no | `true` | After a successful notebook run, release the unmarked processed dataset (`next`). Omit / `null` / `true`: release (default). `false`: leave `next` unreleased and skip the Airtable processed-date stamp. **Cron jobs need no change.** If release (or the notebook) fails, the job **exits non-zero** so Cloud Scheduler retries. A retry with unchanged GCS still calls `release()` when processed `next` is pending. |
| `send_slack` | no | `false` | `true`: Slack on start; final summary when a new Redivis version is released. Failures always Slack. |
| `orgs` | **yes** | — | Non-empty list of organization scopes (see below). |

### `orgs[]` fields

| Field | Required | Default | Meaning |
|-------|----------|---------|---------|
| `org_id` | **yes** | — | Label for this scope (usually the dataset / site id). |
| `is_guest` | **yes** | — | Guest vs registered users. |
| `is_user_id_masked` | no | `false` | Pseudonymize user ids in the export. |
| `user_number_limit` | no | `null` | Cap sampled users (≥ 1). Split 40% students / 40% parents / 20% teachers. |
| `filters` | **yes** | — | At least one of `org_filter`, `date_filter`, `user_filter`. |

### `filters.org_filter`

| Field | Allowed values |
|-------|----------------|
| `key` | `groups` \| `administrations` \| `districts` \| `schools` \| `classes` |
| `operator` | `array_contains_any` only |
| `value` | Non-empty list of strings (max 30) |

### `filters.date_filter`

| Field | Format |
|-------|--------|
| `start_date` | `YYYY-MM-DD` |
| `end_date` | `YYYY-MM-DD` (≥ `start_date`) |

If omitted, Firestore export uses a wide window (`2024-01-01` … `2050-01-01`).

### `filters.user_filter`

| Field | Allowed values |
|-------|----------------|
| `key` | Firestore user field (e.g. `assessmentPid`) |
| `operator` | `starts_with` (string `value`) \| `<=` \| `>=` \| `==` (integer `value`) |
| `value` | string or int, matching the operator |

### Minimal cron-style body (defaults apply)

```json
{
  "dataset_id": "rfp1-sheffield-gb-main-raw",
  "is_save_to_storage": true,
  "send_slack": true,
  "orgs": [
    {
      "org_id": "rfp1-sheffield-gb-main-raw",
      "is_guest": false,
      "filters": {
        "org_filter": {
          "key": "districts",
          "operator": "array_contains_any",
          "value": ["UgLeW6VgHJx1UFVXdrvF"]
        }
      }
    }
  ]
}
```

Runs processing after a new raw release and **releases the processed dataset**.

### Raw only (no notebook)

```json
{
  "dataset_id": "rfp1-sheffield-gb-main-raw",
  "is_save_to_storage": true,
  "skip_process_dataset": true,
  "send_slack": true,
  "orgs": [ { "org_id": "rfp1-sheffield-gb-main-raw", "is_guest": false, "filters": { "org_filter": { "key": "districts", "operator": "array_contains_any", "value": ["UgLeW6VgHJx1UFVXdrvF"] } } } ]
}
```

### Notebook but do not release processed `next`

```json
{
  "dataset_id": "rfp1-sheffield-gb-main-raw",
  "is_save_to_storage": true,
  "release_processed_dataset": false,
  "send_slack": true,
  "orgs": [ { "org_id": "rfp1-sheffield-gb-main-raw", "is_guest": false, "filters": { "org_filter": { "key": "districts", "operator": "array_contains_any", "value": ["UgLeW6VgHJx1UFVXdrvF"] } } } ]
}
```

### Validate only (no GCS / Redivis)

```json
{
  "dataset_id": "rfp1-sheffield-gb-main-raw",
  "is_save_to_storage": false,
  "send_slack": false,
  "orgs": [ { "org_id": "rfp1-sheffield-gb-main-raw", "is_guest": false, "filters": { "org_filter": { "key": "districts", "operator": "array_contains_any", "value": ["UgLeW6VgHJx1UFVXdrvF"] } } } ]
}
```

### Guest + date + user filter (second org)

```json
{
  "org_id": "CO-pre-pilot",
  "is_guest": true,
  "filters": {
    "date_filter": {
      "start_date": "2024-04-01",
      "end_date": "2024-06-30"
    },
    "user_filter": {
      "key": "assessmentPid",
      "operator": "starts_with",
      "value": "col"
    }
  }
}
```

---

## `weekly_report`

```json
{
  "operation": "weekly_report",
  "dry_run": false
}
```

| Field | Required | Default | Meaning |
|-------|----------|---------|---------|
| `operation` | **yes** | — | Must be `weekly_report`. |
| `dry_run` | no | `false` | `true`: assemble the report without posting Slack. |

---

## `open_assignments_sync`

```json
{
  "operation": "open_assignments_sync",
  "dry_run": false
}
```

| Field | Required | Default | Meaning |
|-------|----------|---------|---------|
| `operation` | **yes** | — | Must be `open_assignments_sync`. |
| `dry_run` | no | `false` | `true`: read Airtable / Firestore only; no Airtable writes. Slack still lists sites. |

---

## `redivis_individual_release`

```json
{
  "operation": "redivis_individual_release",
  "dry_run": false,
  "dataset_name": "pilot-uniandes-co-bogota"
}
```

| Field | Required | Default | Meaning |
|-------|----------|---------|---------|
| `operation` | **yes** | — | Must be `redivis_individual_release`. |
| `dry_run` | no | `false` | `true`: no Airtable / Scheduler / Redivis writes. |
| `dataset_name` | no | all sites | Limit to one Airtable dataset `Name` (processed / unmarked). Omit to process the full table. |

---

## `special_dataset_validation`

Live (writes the named raw + processed datasets):

```json
{
  "operation": "special_dataset_validation",
  "dataset_name": "levante-data-pilots-raw"
}
```

Test mode (same Airtable scopes, output redirected):

```json
{
  "operation": "special_dataset_validation",
  "dataset_name": "levante-data-pilots-raw",
  "test_mode": true,
  "test_dataset_name": "TEST-ethan-special-dataset-raw"
}
```

| Field | Required | Default | Meaning |
|-------|----------|---------|---------|
| `operation` | **yes** | — | Must be `special_dataset_validation`. |
| `dataset_name` | **yes** | — | Airtable special-dataset raw name; must end with `-raw`. |
| `test_mode` | no | `false` | `true`: write to `test_dataset_name` instead; skip Airtable `processed_ref_id` writeback. |
| `test_dataset_name` | if `test_mode` | — | Must start with `TEST-` and end with `-raw`. |

Uses default `release_processed_dataset=true` on the inner validation run.

---

## `migrate_scheduler_jobs`

One-time helper to retarget Cloud Scheduler jobs at the Run Job API.

```json
{
  "operation": "migrate_scheduler_jobs",
  "dry_run": true
}
```

| Field | Required | Default | Meaning |
|-------|----------|---------|---------|
| `operation` | **yes** | — | Must be `migrate_scheduler_jobs`. |
| `dry_run` | no | `false` | `true`: list jobs that would migrate; no updates. |

---

## How to send

**HTTP trigger** (returns 202; job runs in Cloud Run):

```
POST https://data-validator-trigger-<hash>-uc.a.run.app/
Content-Type: application/json
API-Key: <validator API key>
```

**Local job process:**

```bash
export project_id=hs-levante-data-validator
export LOCAL_ADMIN_SERVICE_ACCOUNT=/path/to/admin_sa.json
python main.py payload.json
```
