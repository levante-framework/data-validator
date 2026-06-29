# data-validator

## Overview

`data-validator` is a Flask-based Cloud Function that pulls ROAR/LEVANTE data from
Firestore, validates it with Pydantic models, and writes the validated tables to
GCS. When configured, it also publishes a new Redivis dataset version.

High-level flow:
1. Fetch users/runs/trials/surveys and org metadata from Firestore.
2. Validate and normalize with Pydantic models.
3. Write JSON tables to a GCS bucket.
4. Optionally create/release a Redivis dataset version.

## Features

- **Data extraction** from Firestore for users, runs, trials, surveys, and org entities.
- **Schema validation** with Pydantic plus project-specific rules.
- **GCS export** of validated tables and invalid rows.
- **Redivis publish** for new dataset versions when needed.

## Requirements

- Python 3.x
- Pydantic
- Access to ROAR Firebase/Redivis

Install dependencies:
```
pip install -r requirements.txt
```

## API

### Endpoint

The Cloud Function entry point is `data_validator` in `main.py`.

```
POST https://us-central1-{project_id}.cloudfunctions.net/data-validator
```

### Headers

```
Content-Type: application/json
API-Key: <validator api key>
```

### Request body

```
{
    "dataset_id": "CO-bogota-rural-DE-CA",
    "is_save_to_storage": true,
    "is_force_uploading_to_redivis": true,
    "orgs":[
        {
            "org_id": "CO-bogota",
            "is_guest": false,
            "filters":{
                "org_filter":{
                    "key": "districts",
                    "operator": "array_contains_any",
                    "value": ["kdCe535D1FGOtYp8YmYy"]
                    },
                "date_filter":{
                    "start_date": "2024-01-01", 
                    "end_date": "2025-06-30"
                    }
            }
        },
        {
            "org_id": "CO-rural",
            "is_guest": false,
            "filters":{
                "org_filter":{
                    "key": "districts",
                    "operator": "array_contains_any",
                    "value": ["mnmxUgnG1JGYmCRFY7sM"]
                    },
                "date_filter":{
                    "start_date": "2024-01-01", 
                    "end_date": "2025-06-30"
                    }
            }
        },
        {
            "org_id": "CO-pre-pilot", 
            "is_guest": true,
            "filters":{
                "date_filter":{
                    "start_date": "2024-04-01", 
                    "end_date": "2024-06-30"
                    },
                "user_filter":{
                    "key": "assessmentPid",
                    "operator": "starts_with",
                    "value": "col"
                    }
            }
        },
        {
            "org_id": "DE-main",
            "is_guest": false,
            "filters":{
                "org_filter":{
                    "key": "districts",
                    "operator": "array_contains_any",
                    "value": ["x5gHQylxzyACFHohApYY"]
                    }
            }
        },
        {
            "org_id": "CA-main",
            "is_guest": false,
            "filters":{
                "org_filter":{
                    "key": "districts",
                    "operator": "array_contains_any",
                    "value": ["T4e5m4X3McNmBeEMN6ET"]
                    }
            }
        }
    ]
}
```

Notes:
- Required fields: `dataset_id`, `is_save_to_storage`, `orgs` (non-empty list). Optional: `is_force_uploading_to_redivis` and `send_slack` (default `false`).
- Each `orgs[]` item must include `org_id`, `is_guest`, and `filters`. Optional per org: `is_user_id_masked` (defaults to `false`), `user_number_limit` (omit for no cap). `filters` may only contain `org_filter`, `date_filter`, and/or `user_filter`; at least one must be present. Each filter object must only use the allowed keys for that filter (`key`/`operator`/`value` for org and user filters; `start_date`/`end_date` for date filter).
- `send_slack`: if `true`, posts Slack when the job starts, per-site progress (multi-org runs), and a final summary on success. Failures always post to Slack regardless of this flag.
- **Fire-and-forget:** every default `data_validation` request returns HTTP **202** immediately and runs in the background. Cron / Cloud Scheduler should rely on Slack (not the HTTP response body) for outcomes. Deploy with `--no-cpu-throttling`. Maximum runtime is **3600s** per HTTP instance.
- **`start_batch_job`:** for combined / all-sites exports that may exceed 1 hour, set `"operation": "start_batch_job"` with the same body fields as `data_validation`. Starts the **Cloud Run Job** `data-validator-batch` (task timeout **24h**). Returns **202** immediately; monitor via Slack.
- `org_filter.key` must be one of `groups`, `administrations`, `districts`, `schools`, `classes`.
- If `is_save_to_storage` is `false`, the function validates and returns stats only.

## Local development

Set environment variables for local credentials:

```
export LOCAL_ADMIN_SERVICE_ACCOUNT=/path/to/admin_sa.json
```

Then run:
```
flask --app main run
```

```
gcloud config set project gse-roar-admin/gcloud config set project hs-levante-admin-prod

gcloud functions deploy data-validator --gen2 --region us-central1 --runtime python312 --trigger-http --memory=32GiB --timeout 3600s --no-cpu-throttling --allow-unauthenticated --entry-point data_validator
```

## Deployment

```
gcloud config set project <project_id>
gcloud functions deploy data-validator \
  --gen2 \
  --region us-central1 \
  --runtime python312 \
  --trigger-http \
  --memory=32GiB \
  --timeout 3600s \
  --no-cpu-throttling \
  --allow-unauthenticated \
  --entry-point data_validator

gcloud run jobs deploy data-validator-batch \
  --source . \
  --region us-central1 \
  --memory 32Gi \
  --cpu 8 \
  --task-timeout 86400s \
  --max-retries 0 \
  --command python \
  --args batch_main.py
```

## Acknowledgments

- ROAR and LEVANTE team
