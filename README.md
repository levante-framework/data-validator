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
git clone https://github.com/levante-framework/data-validator.git
```
2. Install dependencies:
```
pip install -r requirements.txt
```
### Usage

1. Configure your Firebase/Redivis access credentials.
2. Send HTTP request to this API deployed on GCP:
```angular2html
https://...
```
1. Include api_key in request header.
2. Include followings in json format. 
{
    "lab_id": "61e8aee84cf0e71b14295d45",
    "source": "firestore" 
}

### Debug and Deployment

local: 
```
functions-framework --target=data_validator 
```
Then send request to http://localhost:8080/

deploy to cloud:
```
gcloud config set project hs-levante-admin-dev
gcloud functions deploy data-validator --runtime python312 --trigger-http --allow-unauthenticated --entry-point data_validator
```
https://us-central1-hs-levante-admin-dev.cloudfunctions.net/data-validator
## Acknowledgments

- ROAR and LEVANTE team