import os
import re
import json
import math
import settings
import logging
import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials
from google.cloud import secretmanager


def params_check(lab_id, is_save_to_storage, is_upload_to_redivis, is_release_on_redivis):
    if not lab_id:
        return "Parameter 'lab_id' needs to be specified.", 400
    elif not isinstance(lab_id, str):
        return "Parameter 'lab_id' needs to be a valid string.", 400
    if not isinstance(is_save_to_storage, bool):
        return "Parameter 'is_save_to_storage' has to be a bool value.", 400
    if not isinstance(is_upload_to_redivis, bool):
        return "Parameter 'is_upload_to_redivis' has to be a bool value.", 400
    if not isinstance(is_release_on_redivis, bool):
        return "Parameter 'is_release_on_redivis' has to be a bool value.", 400

    return True


# Utility function for converting dictionaries to snake_case and handling NaN values
def process_doc_dict(doc_dict, ignore_keys=None):
    if ignore_keys is None:
        ignore_keys = []
    converted_doc_dict = {camel_to_snake(key): handle_nan(value) for key, value in doc_dict.items()
                          if key not in ignore_keys}
    return converted_doc_dict


def camel_to_snake(camel_str):
    # Find all matches where a lowercase letter is followed by an uppercase letter
    matches = re.finditer(r'([a-z])([A-Z])', camel_str)

    # Insert an underscore before each uppercase letter
    for match in matches:
        camel_str = camel_str.replace(match.group(), match.group(1) + '_' + match.group(2))

    # Convert the entire string to lowercase
    snake_str = camel_str.lower()

    return snake_str


def handle_nan(value):
    if isinstance(value, float) and math.isnan(value):
        return None if settings.config['INSTANCE'] == 'LEVANTE' else "NaN"
    elif isinstance(value, dict):
        # Recursively handle NaN values in nested dictionaries
        return {key: handle_nan(val) for key, val in value.items()}
    elif isinstance(value, list):
        # Recursively handle NaN values in nested lists
        return [handle_nan(val) for val in value]
    return value


def ids_to_names(id_list: list, obj_list):
    # Create a dictionary to map ids to names for faster lookup
    id_to_name_map = {obj.id: obj.name for obj in obj_list}

    # Map each id in id_list to its corresponding name using the dictionary
    # If an id is not found, append None to the result list
    names = [id_to_name_map.get(id) for id in id_list]

    return names


# Get the Secret Manager client
def get_secret_manager_client():
    if os.getenv('ENV') == 'local':
        logging.info("Running in local mode.")
        return secretmanager.SecretManagerServiceClient.from_service_account_json(settings.local_admin_service_account)
    else:
        logging.info("Running in remote mode.")
        return secretmanager.SecretManagerServiceClient()


# Get the secrets and decode the data
def get_secret(secret_id, secret_client):
    return secret_client.access_secret_version(
        request={"name": f"projects/{settings.project_id}/secrets/{secret_id}/versions/latest"}
    ).payload.data.decode("UTF-8")


# Get the service account and initialize the Firebase app
def initialize_firebase(service_account):
    credential = credentials.Certificate(json.loads(service_account))
    return firebase_admin.initialize_app(credential=credential, name="admin")


def get_lab_ids_from_payload(request_json):
    lab_ids = request_json.get('lab_ids', [])
    logging.info(f"Found {len(lab_ids)} lab_ids in request: {lab_ids}")
    return lab_ids


def get_lab_ids_from_firestore(app):
    db = firestore.client(app)
    docs = db.collection("districts").get()
    # Filter out test data
    lab_ids = [doc.id for doc in docs if not doc.to_dict().get('testData')]
    logging.info(f"Found {len(lab_ids)} docs in Firestore: {lab_ids}")
    return lab_ids


# Get lab_ids from request or Firestore; request for testing locally
def get_lab_ids(_request, app):
    if _request and _request.method == 'POST':
        request_json = _request.get_json(silent=True)
        if request_json.get('is_test', False):
            # This clause allows testing the function locally with a POST request
            logging.info("Running in test mode with preset lab_ids.")
            return get_lab_ids_from_payload(request_json)
        else:
            # This clause allows triggering the function manually with a POST request
            logging.info("Running in manual mode with lab_ids from Firestore.")
            return get_lab_ids_from_firestore(app)
    else:
        # This clause allows the function to run as a scheduled Cloud job
        logging.info("Running in scheduled mode with lab_ids from Firestore.")
        return get_lab_ids_from_firestore(app)
