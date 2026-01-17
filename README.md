# data-validator

## Overview

This project, `data-validator`, is designed to facilitate the processing and validation of data from ROAR Firebase/Redivis(2nd attempt). 
It uses Pydantic for validation along with customized restrictions to ensure data integrity. 
Once validated, the data is submitted back to Redivis.

## Features

- **Data Extraction**: Extract data efficiently from ROAR Firebase/Redivis.
- **Data Validation**: Leverage Pydantic along with custom validations to ensure the accuracy and integrity of the data.
- **Data Storage**: Seamlessly submit the validated data and invalid data_log to Gcloud storage.

## Getting Started

### Prerequisites

- Python 3.x
- Pydantic
- Access to ROAR Firebase/Redivis

### Installation

1. Clone the repository:
```
git clone https://github.com/yeatmanlab/data-validator.git
```
2. Install dependencies:
```
pip install -r requirements.txt
```
### Usage

1. Configure your Firebase/Redivis access credentials.
2. Replace the creds files path in settings.py to your GCP project ones.
3. Send HTTP request to this API deployed on GCP:
```angular2html
https://us-central1-gse-roar-admin.cloudfunctions.net/data-validator
```
1. Include api_key in request header.
    --header 'Content-Type: application/json' 
    --header 'API-Key: ..'
2. Include followings in json format. 
```
{
    "dataset_id": "US-downward_extension-pilot",
    "is_save_to_storage": true,
    "is_force_uploading_to_redivis": true,
    "orgs":[
        {
            "org_id": "US-downward_extension-pilot",
            "is_guest": false,
            "filters":{
                "org_filter":{
                    "key": "groups",
                    "operator": "array_contains_any",
                    "value": ["Bing Nursery School", "JMZ Pilot"]
                    }
            }
        }
    ]
}
```
### Debug and Deployment

local: 
```
flask --app main run
```
Then send request to http://127.0.0.1:5000

deploy data-validator to cloud:
```
gcloud config set project gse-roar-admin/gcloud config set project hs-levante-admin-prod

gcloud functions deploy data-validator --gen2 --region us-central1 --runtime python312 --trigger-http --memory=32GiB --timeout 3600s --allow-unauthenticated --entry-point data_validator
```
Then send request to
```
https://us-central1-{project_id}.cloudfunctions.net/data-validator
```

## Acknowledgments

- ROAR and LEVANTE team
