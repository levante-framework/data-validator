version = '0.0.1'

DB_SITE = 'local-dev'  # local-dev, local-prod, remote
validation_api_url = ''
BUCKET_NAME = "levante-roar-raw-data-bucket-dev" #levante-roar-raw-data-bucket-prod

test_mode = True  # True, False
SAVE_TO_STORAGE = False
SAVE_TO_DB = False

DB_KEY_LOCATION_ADMIN = "../secrets/hs-levante-admin-dev-db.json"
SA_KEY_LOCATION_ADMIN = "../secrets/hs-levante-admin-dev-sa.json"
DB_KEY_LOCATION_ASSESSMENT = "../secrets/hs-levante-assessment-dev-db.json"
PROJECT_ID_ASSESSMENT = "hs-levante-assessment-dev"
