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


def params_check(lab_ids, is_from_guest, is_save_to_storage, is_upload_to_redivis, is_release_on_redivis, filter_by,
                 filter_list, start_date, end_date):
    # Validate 'lab_ids'
    if not lab_ids:
        return False, "Parameter 'lab_ids' needs to be specified."
    elif not isinstance(lab_ids, list):
        return False, "Parameter 'lab_ids' needs to be a valid list."
    else:
        pattern = re.compile("^[a-zA-Z0-9-]+$")
        for lab_id in lab_ids:
            if not isinstance(lab_id, str) or not pattern.match(lab_id):
                return False, "All elements in 'lab_ids' must be strings containing only letters, numbers, and hyphens."

    # Validate boolean parameters
    if not isinstance(is_from_guest, bool):
        return False, "Parameter 'is_from_guest' has to be a bool value."
    if not isinstance(is_save_to_storage, bool):
        return False, "Parameter 'is_save_to_storage' has to be a bool value."
    if not isinstance(is_upload_to_redivis, bool):
        return False, "Parameter 'is_upload_to_redivis' has to be a bool value."
    if not isinstance(is_release_on_redivis, bool):
        return False, "Parameter 'is_release_on_redivis' has to be a bool value."

    # Validate 'filter_by' and 'filter_list'
    if (filter_by is not None and filter_list is None) or (filter_by is None and filter_list is not None):
        return False, "'filter_by' and 'filter_list' must either both be specified or not specified."
    if filter_by is not None and filter_by not in ["groups", "districts", "schools"]:
        return False, "Parameter 'filter_by' must be one of the following values: ['groups', 'districts', 'schools']."
    if filter_list is not None and not all(isinstance(item, str) for item in filter_list):
        return False, "All elements in 'filter_list' must be strings."

    # Validate 'start_date' and 'end_date'
    date_pattern = re.compile(r"^202\d-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$")
    if start_date is not None and not (isinstance(start_date, str) and date_pattern.match(start_date)):
        return False, "Parameter 'start_date' should be a string in the format 'YYYY-MM-DD'."
    if end_date is not None and not (isinstance(end_date, str) and date_pattern.match(end_date)):
        return False, "Parameter 'end_date' should be a string in the format 'YYYY-MM-DD'."

    return True, ""


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
