from dotenv import load_dotenv
import os

load_dotenv()

version = '0.0.1'

ENV = 'remote'  # local-dev, local-prod, remote

CORE_DATA_BUCKET_NAME = 'firebase-redivis-pipeline'
EXTERNAL_DATA_BUCKET_NAME = 'firebase-redivis-pipeline-external'

assessment_service_account_secret_id = 'assessmentPrivateKey'
admin_firebase_api_key_secret_id = 'adminPublicKey'

# DB_KEY_LOCATION_ADMIN = "../secrets/hs-levante-admin-dev-db.json"
SA_KEY_LOCATION_ADMIN = '../../firebase/admin-credentials.json'

redivis_api_token = os.getenv('FIREBASE_REDIVIS_PIPELINE_ACCESS_TOKEN')
# Store secret in Secret Manager for gse-roar-admin project
redivis_api_token_secret_id = 'firebaseRedivisPipelineAccessToken'
