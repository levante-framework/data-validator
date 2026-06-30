# data-validator

## Overview

`data-validator` runs as a **Cloud Run Job**. It pulls ROAR/LEVANTE data from
Firestore, validates it with Pydantic models, and writes the validated tables to
GCS. When configured, it also publishes a new Redivis dataset version.

High-level flow:
1. Fetch users/runs/trials/surveys and org metadata from Firestore.
2. Validate and normalize with Pydantic models.
3. Write JSON tables to a GCS bucket.
4. Optionally create/release a Redivis dataset version.

Daily per-site cron jobs are **Cloud Scheduler → Run Job API** triggers (not HTTP
to a Cloud Function).

## Features

- **Data extraction** from Firestore for users, runs, trials, surveys, and org entities.
- **Schema validation** with Pydantic plus project-specific rules.
- **GCS export** of validated tables and invalid rows.
- **Redivis publish** for new dataset versions when needed.
- **Slack notifications** on job start, per-org progress (multi-org runs), and final summary.

## Requirements

- Python 3.12+
- Docker (for local container runs)
- Access to LEVANTE Firebase / Redivis / GCP

Install dependencies locally:

```bash
pip install -r requirements.txt
```

## Job payload

Set environment variable `DATA_VALIDATOR_PAYLOAD` to a JSON object.

Optional top-level `operation` (default `data_validation`):

| `operation` | Purpose |
|-------------|---------|
| `data_validation` | Full validate → GCS → Redivis pipeline |
| `open_assignments_sync` | Sync open assignments from Airtable |
| `weekly_report` | Weekly ops report to Slack |
| `redivis_individual_release` | Per-site scheduler + Redivis provisioning |

### Data validation example

```json
{
  "dataset_id": "CO-bogota-rural-DE-CA",
  "is_save_to_storage": true,
  "is_force_uploading_to_redivis": true,
  "send_slack": true,
  "orgs": [
    {
      "org_id": "CO-bogota",
      "is_guest": false,
      "filters": {
        "org_filter": {
          "key": "districts",
          "operator": "array_contains_any",
          "value": ["kdCe535D1FGOtYp8YmYy"]
        },
        "date_filter": {
          "start_date": "2024-01-01",
          "end_date": "2025-12-31"
        }
      }
    }
  ]
}
```

Notes:
- Required fields: `dataset_id`, `is_save_to_storage`, `orgs` (non-empty list).
- `send_slack`: if `true`, posts Slack when the job starts, per-site progress (multi-org), and a final summary. Failures always post to Slack.
- Task timeout: **24 hours** (`86400s`).
- If `is_save_to_storage` is `false`, the job validates and returns stats only.

### Auxiliary operations

```json
{"operation": "weekly_report", "dry_run": false}
```

```json
{"operation": "redivis_individual_release", "dry_run": false, "dataset_name": "optional-single-site"}
```

```json
{"operation": "open_assignments_sync", "dry_run": false}
```

## Local development

### Python (no Docker)

```bash
export project_id=hs-levante-data-validator
export LOCAL_ADMIN_SERVICE_ACCOUNT=/path/to/admin_sa.json

export DATA_VALIDATOR_PAYLOAD='{"dataset_id":"...", "is_save_to_storage":false, "orgs":[...]}'
python main.py
```

### Docker (matches Cloud Run Job)

Build:

```bash
docker build -t data-validator .
```

Run validation (mount your service account):

```bash
docker run --rm \
  -e project_id=hs-levante-data-validator \
  -e LOCAL_ADMIN_SERVICE_ACCOUNT=/secrets/admin.json \
  -e DATA_VALIDATOR_PAYLOAD='{"dataset_id":"MY-SITE","is_save_to_storage":false,"send_slack":false,"orgs":[{"org_id":"MY-SITE","is_guest":false,"filters":{"date_filter":{"start_date":"2024-01-01","end_date":"2025-12-31"}}}]}' \
  -v /path/to/admin_sa.json:/secrets/admin.json:ro \
  data-validator
```

Exit code `0` = success; non-zero = failure (Slack crash alert on failure when `send_slack` is true).

## Triggering in GCP

**There is no HTTP URL anymore.** The retired Cloud Function URL
(`https://us-central1-….cloudfunctions.net/data-validator`) is replaced by the
Cloud Run Job `data-validator`.

### Manual run (gcloud)

```bash
gcloud config set project hs-levante-data-validator

gcloud run jobs execute data-validator \
  --region us-central1 \
  --update-env-vars DATA_VALIDATOR_PAYLOAD='{"dataset_id":"...", ...}'
```

For large payloads, put JSON in a file and use:

```bash
PAYLOAD=$(python3 -c "import json; print(json.dumps(json.load(open('payload.json')), separators=(',', ':')))")
gcloud run jobs execute data-validator \
  --region us-central1 \
  --update-env-vars "DATA_VALIDATOR_PAYLOAD=${PAYLOAD}"
```

Monitor execution:

```bash
gcloud run jobs executions list --job data-validator --region us-central1
gcloud logging read 'resource.type="cloud_run_job" AND resource.labels.job_name="data-validator"' --limit 50
```

### Postman / manual API trigger

There is **no** Cloud Function URL anymore. To trigger from Postman (or curl), call the
**Cloud Run Admin API** with a Google OAuth bearer token:

```
POST https://run.googleapis.com/v2/projects/hs-levante-data-validator/locations/us-central1/jobs/data-validator:run
Authorization: Bearer <access_token>
Content-Type: application/json
```

Body (per-site validation example):

```json
{
  "overrides": {
    "containerOverrides": [
      {
        "env": [
          {
            "name": "DATA_VALIDATOR_PAYLOAD",
            "value": "{\"dataset_id\":\"pilot-uniandes-co-bogota\",\"is_save_to_storage\":false,\"send_slack\":true,\"orgs\":[{\"org_id\":\"pilot-uniandes-co-bogota\",\"is_guest\":false,\"filters\":{\"org_filter\":{\"key\":\"districts\",\"operator\":\"array_contains_any\",\"value\":[\"SITE_ID_HERE\"]},\"date_filter\":{\"start_date\":\"2024-01-01\",\"end_date\":\"2025-12-31\"}}}]}"
          }
        ]
      }
    ]
  }
}
```

Get an access token locally:

```bash
gcloud auth print-access-token
```

Your Google account (or service account) needs permission to run the job
(`roles/run.developer` or `run.jobs.run`).

### Migrate existing Cloud Scheduler jobs

After deploy, run once to retarget legacy Cloud Function cron jobs:

```bash
gcloud run jobs execute data-validator \
  --region us-central1 \
  --update-env-vars DATA_VALIDATOR_PAYLOAD='{"operation":"migrate_scheduler_jobs"}'
```

### Daily per-site cron (Cloud Scheduler)

`redivis_individual_release` creates or **migrates** Cloud Scheduler jobs to POST
to the **Run Job API**:

`https://run.googleapis.com/v2/projects/{project}/locations/us-central1/jobs/data-validator:run`

with OAuth (scheduler service account). Re-run `redivis_individual_release` after
deploy to migrate existing jobs off the old Cloud Function URL.

The scheduler OAuth service account needs permission to run the job
(`roles/run.developer` or `run.jobs.run`). Default: `{project_id}@appspot.gserviceaccount.com`
(override via `CLOUD_SCHEDULER_OAUTH_SERVICE_ACCOUNT` in `settings.py`).

## Deployment

GitHub Actions (`.github/workflows/deploy_to_gcf.yml`) deploys on push to `main` or
`dev`. The deploy service account needs:

- `roles/run.admin`, `roles/iam.serviceAccountUser`, `roles/storage.admin`
- `roles/cloudbuild.builds.editor`, `roles/artifactregistry.writer` (for `--source` deploy)
- `roles/cloudfunctions.admin` (only to delete the retired gen2 function)

Manual deploy:

```bash
gcloud run jobs deploy data-validator \
  --source . \
  --region us-central1 \
  --memory 32Gi \
  --cpu 8 \
  --task-timeout 86400s \
  --max-retries 0 \
  --command python \
  --args main.py
```

## Slack

Unchanged behavior for validation jobs:

- Job started message when `send_slack: true`
- Per-org progress for multi-org runs
- Final summary on success
- Crash / failure alerts always posted for validation failures

Auxiliary operations (`weekly_report`, `redivis_individual_release`) use their
existing Slack formatters.

## Acknowledgments

- ROAR and LEVANTE team
