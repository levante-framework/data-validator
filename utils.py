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


def params_check(dataset_id, is_save_to_storage, is_upload_to_redivis, is_release_on_redivis, orgs):
    date_pattern = re.compile(r"^202\d-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$")
    id_pattern = re.compile("^[a-zA-Z0-9-]+$")

    # Validate 'dataset_id'
    if not dataset_id:
        return False, "Parameter 'dataset_id' needs to be specified."
    if not isinstance(dataset_id, str) or not id_pattern.match(dataset_id):
        return False, "'dataset_id' must be strings containing only letters, numbers, and hyphens."

    # Validate boolean parameters
    if not isinstance(is_save_to_storage, bool):
        return False, "Parameter 'is_save_to_storage' has to be a bool value."
    if not isinstance(is_upload_to_redivis, bool):
        return False, "Parameter 'is_upload_to_redivis' has to be a bool value."
    if not isinstance(is_release_on_redivis, bool):
        return False, "Parameter 'is_release_on_redivis' has to be a bool value."
    if not isinstance(orgs, list):
        return False, "Parameter 'orgs' and 'guests' have to be list values."

    # Helper function to validate orgs or guests
    def validate_org(orgs):
        for org in orgs:
            if not isinstance(org, dict):
                return False, "Each org in the orgs must be a dictionary."

            # Extract information from item
            org_id = org.get('org_id', None)
            is_guest = org.get('is_guest', False)
            start_date = org.get('start_date', None)
            end_date = org.get('end_date', None)
            primary_filter = org.get("filters", {}).get("primary", {})
            filter_key = primary_filter.get('key', None)
            filter_operator = primary_filter.get('operator', None)
            filter_value = primary_filter.get('value', None)

            # Validate 'org_id'
            if not org_id:
                return False, "Parameter 'org_id' needs to be specified."
            if not isinstance(org_id, str) or not id_pattern.match(org_id):
                return False, "'org_id' must be strings containing only letters, numbers, and hyphens."

            if not isinstance(is_guest, bool):
                return False, "Parameter 'is_guest' has to be a bool value."

            # Validate filtering parameters
            if any([filter_operator, filter_key, filter_value]) and not all([filter_operator, filter_key, filter_value]):
                return False, "Filter parameters must either all be provided or none should be provided."
            if filter_key and not isinstance(filter_key, str):
                return False, "'filter_key' must be a string."

            # Specific filter rules
            if filter_operator:
                if filter_operator not in ["str_contains_str", "array_contains_str", "array_contains_any"]:
                    return False, f"filter_operator must be in ['str_contains_str', 'array_contains_str', 'array_contains_any]"
                if filter_operator in ["str_contains_str", "array_contains_str"] and not isinstance(filter_value, str):
                    return False, f"'filter_value' must be a string for filter {filter_operator}."
                elif filter_operator == "array_contains_any":
                    if not isinstance(filter_value, list) or not all(isinstance(i, str) for i in filter_value):
                        return False, "'filter_value' must be an array of strings for filter 'array_contains_any'."

            # Date validations
            if start_date is not None and not (isinstance(start_date, str) and date_pattern.match(start_date)):
                return False, "Parameter 'start_date' should be a string in the format 'YYYY-MM-DD'."
            if end_date is not None and not (isinstance(end_date, str) and date_pattern.match(end_date)):
                return False, "Parameter 'end_date' should be a string in the format 'YYYY-MM-DD'."

        return True, "Validation successful."

    # Validate orgs and guests
    valid_orgs = validate_org(orgs)
    if not valid_orgs[0]:
        return valid_orgs

    return True, "All parameters are valid."


def merge_dictionaries(dict1, dict2):
    # Initialize the result dictionary
    merged_dict = {}

    # Get all keys from both dictionaries
    all_keys = set(dict1.keys()).union(dict2.keys())

    # Iterate over all keys
    for key in all_keys:
        # If key is in both dictionaries, concatenate the lists
        if key in dict1 and key in dict2:
            merged_dict[key] = dict1[key] + dict2[key]
        # If key is only in the first dictionary, add it directly
        elif key in dict1:
            merged_dict[key] = dict1[key]
        # If key is only in the second dictionary, add it directly
        elif key in dict2:
            merged_dict[key] = dict2[key]

    return merged_dict


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


def generate_query_description(table_name, collection_source, date_field=None, start_date=None, end_date=None,
                               filter_field=None, filter_list=None):
    description = {
        'table_name': table_name,
        'collection_source': collection_source,
        'date_field': date_field,
        'Start Date': start_date,
        'End Date': end_date
    }
    if filter_field and filter_list:
        description['Filter Field'] = filter_field
        description['Filter Values'] = filter_list

    return description
