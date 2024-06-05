from dotenv import load_dotenv
import os

load_dotenv()

version = '0.0.1'

ENV = 'local'  # local, remote

CORE_DATA_BUCKET_NAME = 'firebase-redivis-pipeline'
EXTERNAL_DATA_BUCKET_NAME = 'firebase-redivis-pipeline-external'

assessment_service_account_secret_id = 'assessmentPrivateKey'
admin_firebase_api_key_secret_id = 'adminPublicKey'

# Used when running locally
SA_KEY_LOCATION_ADMIN = '../../firebase/admin-credentials.json'

redivis_api_token = os.getenv('FIREBASE_REDIVIS_PIPELINE_ACCESS_TOKEN')
# Secret Manager secret ID for the Redivis API token in gse-roar-admin project
redivis_api_token_secret_id = 'firebaseRedivisPipelineAccessToken'
