from dotenv import load_dotenv
import os
import json
import requests

# Global configuration variables
config = {
    'VERSION': '1.0.0',
    'INSTANCE': 'ROAR',
    'CORE_DATA_BUCKET_NAME': 'firebase-redivis-pipeline',
    'EXTERNAL_DATA_BUCKET_NAME': 'firebase-redivis-pipeline-external',
    'ADMIN_SERVICE_ACCOUNT_SECRET_ID': 'adminServiceAccount',
    'ADMIN_PUBLIC_KEY_SECRET_ID': 'adminPublicKey',
    'DATA_VALIDATOR_URL_SECRET_ID': "dataValidatorUrl",
    'ASSESSMENT_SERVICE_ACCOUNT_SECRET_ID': 'assessmentPrivateKey',
    'VALIDATOR_API_SECRET_ID': 'adminPublicKey',
    'REDIVIS_API_TOKEN_SECRET_ID': 'firebaseRedivisPipelineAccessToken',
    'REDIVIS_IDENTITY_ACCOUNT_SECRET_ID': 'redivisIdentityEmailAccount'
}


def initialize_env_securities():
    global config
    try:
        response = requests.get(
            "http://metadata.google.internal/computeMetadata/v1/project/project-id",
            headers={"Metadata-Flavor": "Google"},
            timeout=2
        )
        if response.status_code == 200:
            project_id = response.text
            os.environ['project_id'] = project_id
            os.environ['ENV'] = "remote"
    except requests.exceptions.RequestException:
        load_dotenv()
        os.environ['ENV'] = "local"
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('LOCAL_ADMIN_SERVICE_ACCOUNT')
        with open(os.getenv('LOCAL_ADMIN_SERVICE_ACCOUNT'), 'r') as sa:
            os.environ['project_id'] = json.load(sa).get('project_id', None)

    if 'levante' in os.environ['project_id']:
        config['INSTANCE'] = 'LEVANTE'
        config['CORE_DATA_BUCKET_NAME'] = 'levante-roar-data-bucket-dev'
        config['EXTERNAL_DATA_BUCKET_NAME'] = 'levante-external-data'
        config['ASSESSMENT_SERVICE_ACCOUNT_SECRET_ID'] = 'assessmentServiceAccount'
        config['VALIDATOR_API_SECRET_ID'] = 'validatorApiKey'



# # Secret Manager secret ID for various API keys
# admin_service_account_secret_id = "adminServiceAccount"
# admin_public_key_secret_id = "adminPublicKey"
# data_validator_url_secret_id = "dataValidatorUrl"
# assessment_service_account_secret_id = 'assessmentPrivateKey'
# admin_firebase_api_key_secret_id = 'adminPublicKey'
# redivis_api_token_secret_id = 'firebaseRedivisPipelineAccessToken'
#
#
#
# # Used when running locally
# local_admin_service_account = os.getenv('LOCAL_ADMIN_SERVICE_ACCOUNT')
#
