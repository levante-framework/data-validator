version = '0.0.1'

ENV = 'remote'  # local-dev, local-prod, remote

BUCKET_NAME = "levante-roar-data-bucket-dev"  # levante-roar-raw-data-bucket-prod

assessment_service_account_secret_id = 'hs-levante-assessment-dev-service-account'
admin_firebase_api_key_secret_id = 'firebase-api-key'

DB_KEY_LOCATION_ADMIN = "../secrets/hs-levante-admin-dev-db.json"
SA_KEY_LOCATION_ADMIN = "../secrets/hs-levante-admin-dev-sa.json"

redivis_api_token = 'AAAChAdeK/oVg/jVDmR6QFxuXvqVrEm4'

# functions-framework --target=main
# gcloud config set project hs-levante-admin-dev
# gcloud functions deploy data_validator --runtime python312 --trigger-http --allow-unauthenticated --entry-point main
