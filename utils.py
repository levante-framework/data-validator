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

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Union, Optional
from datetime import datetime


class DateFilter(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    @field_validator('start_date', 'end_date')
    def check_date_format(cls, v: str):
        if v is not None:
            try:
                datetime.strptime(v, '%Y-%m-%d')
                return v
            except ValueError:
                raise ValueError("Date must be in format YYYY-MM-DD")
        return v


class UserFilter(BaseModel):
    key: Optional[str] = Field(default=None)
    operator: Optional[str] = Field(default=None, pattern="^(contains|<=|>=|==)$")
    value: Optional[Union[int, str]] = Field(default=None)

    @model_validator(mode='before')
    def validate_value(cls, data):
        if 'operator' in data:
            if data['operator'] == 'contains' and not isinstance(data['value'], str):
                raise TypeError("Value must be a string when operator is 'contains'")
            elif data['operator'] in ['<=', '>=', '=='] and not isinstance(data['value'], int):
                raise TypeError("Value must be an integer when operator is '<=', '>=', or '=='")
        return data

    @model_validator(mode='before')
    def check_all_or_none(cls, values):
        fields = ['key', 'operator', 'value']
        all_none = all(values.get(f) is None for f in fields)
        all_set = all(values.get(f) is not None for f in fields)

        if not (all_none or all_set):
            raise ValueError("All of 'key', 'operator', and 'value' must be set or all must be None")
        return values


class OrgFilter(BaseModel):
    key: Optional[str] = Field(default=None, pattern="^(groups|administrations|districts|schools|classes)$")
    operator: Optional[str] = Field(default=None, pattern="^array_contains_any$")
    value: Optional[List[str]] = None

    @field_validator('value')
    def check_value_type(cls, v):
        if not all(isinstance(element, str) for element in v):
            raise ValueError("Each item in value must be a string")
        return v

    @model_validator(mode='before')
    def check_all_or_none(cls, values):
        fields = ['key', 'operator', 'value']
        all_none = all(values.get(f) is None for f in fields)
        all_set = all(values.get(f) is not None for f in fields)

        if not (all_none or all_set):
            raise ValueError("All of 'key', 'operator', and 'value' must be set or all must be None")
        return values


class Filters(BaseModel):
    date_filter: Optional[DateFilter] = Field(default_factory=DateFilter)
    user_filter: Optional[UserFilter] = Field(default_factory=UserFilter)
    org_filter: Optional[OrgFilter] = Field(default_factory=OrgFilter)

    @model_validator(mode='before')
    def check_allowed_fields(cls, values):
        allowed_fields = {'date_filter', 'user_filter', 'org_filter'}
        extra_fields = set(values) - allowed_fields
        if extra_fields:
            raise ValueError(f"Invalid fields passed: {extra_fields}")
        return values


class Organization(BaseModel):
    org_id: str = Field()
    is_guest: bool = Field()
    filters: Optional[Filters] = Field(default_factory=Filters)


class DatasetParameters(BaseModel):
    dataset_id: str = Field()
    is_save_to_storage: bool = Field()
    is_upload_to_redivis: bool = Field()
    is_release_to_redivis: bool = Field()
    prefix: Optional[str] = None
    orgs: List[Organization] = Field()


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
