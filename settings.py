from dotenv import load_dotenv
import os

load_dotenv()

version = '1.0.0'

ENV = 'local'  # local, remote

CORE_DATA_BUCKET_NAME = 'firebase-redivis-pipeline'
EXTERNAL_DATA_BUCKET_NAME = 'firebase-redivis-pipeline-external'

project_id = "gse-roar-admin"

# Secret Manager secret ID for various API keys
admin_service_account_secret_id = "adminServiceAccount"
admin_public_key_secret_id = "adminPublicKey"
data_validator_url_secret_id = "dataValidatorUrl"
assessment_service_account_secret_id = 'assessmentPrivateKey'
admin_firebase_api_key_secret_id = 'adminPublicKey'
redivis_api_token_secret_id = 'firebaseRedivisPipelineAccessToken'

redivis_api_token = os.getenv('FIREBASE_REDIVIS_PIPELINE_ACCESS_TOKEN')

# Used when running locally
local_admin_service_account = os.getenv('LOCAL_ADMIN_SERVICE_ACCOUNT')

