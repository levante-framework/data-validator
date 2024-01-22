version = '0.0.1'

DB_SITE = 'remote'  # local-dev, local-prod, remote
validation_api_url = ''
BUCKET_NAME = "levante-roar-data-bucket-dev"  # levante-roar-raw-data-bucket-prod
project_id = 'hs-levante-admin-dev'  # hs-levante-admin-prod
project_id_ASSESSMENT = "hs-levante-assessment-dev"

assessment_service_account_secret_id = 'hs-levante-assessment-dev-service-account'
admin_firebase_api_key_secret_id = 'firebase-api-key'

SAVE_TO_STORAGE = True  # True, False

DB_KEY_LOCATION_ADMIN = "../secrets/hs-levante-admin-dev-db.json"
SA_KEY_LOCATION_ADMIN = "../secrets/hs-levante-admin-dev-sa.json"
# DB_KEY_LOCATION_ASSESSMENT = "../secrets/hs-levante-assessment-dev-db.json"

redivis_api_token = 'AAAChAdeK/oVg/jVDmR6QFxuXvqVrEm4'



#functions-framework --target=main
#gcloud config set project hs-levante-admin-dev
#gcloud functions deploy data_validator --runtime python312 --trigger-http --allow-unauthenticated --entry-point main
