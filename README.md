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
- `slack_notification_mode` must be one of `Full`, `New_Schema`, or `None`.
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

gcloud functions deploy data-validator --gen2 --region us-central1 --runtime python312 --trigger-http --memory=32GiB --timeout 3600s --allow-unauthenticated --entry-point data_validator
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
  --allow-unauthenticated \
  --entry-point data_validator
```

## Acknowledgments

- ROAR and LEVANTE team
