import os
import re
import hashlib
import math
import settings
import logging
from dotenv import load_dotenv
import json
import requests

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Union, Optional, Literal
from datetime import datetime


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


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
        elif len(v) > 30:
            raise ValueError("Number of items in value must be less than 30.")
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
    is_save_to_storage: bool = Field(default=False)
    is_force_uploading_to_redivis: bool = Field(default=False)
    slack_notification_mode: Literal['Full', 'New_Schema', 'None'] = Field(default='None')
    orgs: List[Organization] = Field()

    def to_dict(self):
        # Build a shorter description from params
        org_summary = []
        for org in self.orgs:
            # Summarize organization details
            group_names = org.filters.org_filter.value if org.filters.org_filter.value else 'None'
            date_range = f"{org.filters.date_filter.start_date} to {org.filters.date_filter.end_date}" if org.filters.date_filter.start_date and org.filters.date_filter.end_date else "No date limit"
            org_summary.append(
                f"{org.org_id} ({'guests' if org.is_guest else 'users'}): groups ({group_names}), Date range: {date_range}")

        # Join all summaries, but check length constraint
        full_description_org = "; ".join(org_summary)
        if len(full_description_org) > 1950:  # Leave some room for static text
            full_description_org = full_description_org[:1950] + "..."  # Truncate to fit

        return {
            'dataset_id': self.dataset_id,
            'is_save_to_storage': self.is_save_to_storage,
            'is_force_uploading_to_redivis': self.is_force_uploading_to_redivis,
            'slack_notification_mode': self.slack_notification_mode,
            'orgs': full_description_org,
        }


def setup_project_environment():
    print("Setting up project Environments...")
    try:
        response = requests.get(
            "http://metadata.google.internal/computeMetadata/v1/project/project-id",
            headers={"Metadata-Flavor": "Google"},
            timeout=2
        )
        if response.status_code == 200:
            project_id = response.text
            os.environ['project_id'] = project_id
            os.environ['ENV'] = "remote"
    except requests.exceptions.RequestException:
        load_dotenv()
        os.environ['ENV'] = "local"
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('LOCAL_ADMIN_SERVICE_ACCOUNT')
        with open(os.getenv('LOCAL_ADMIN_SERVICE_ACCOUNT'), 'r') as sa:
            os.environ['project_id'] = json.load(sa).get('project_id', None)
    settings.config[
        'CORE_DATA_BUCKET_NAME'] = f'levante-roar-data-bucket-{'dev' if 'dev' in os.environ['project_id'] else 'prod'}'
    print(
        f"running version {settings.config['VERSION']}, "
        f"project_id: {os.getenv('project_id')}, "
        f"instance: {settings.config['INSTANCE']}"
    )


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


def reduce_duplication_by_keys(data: dict, keys: dict):
    processed_data = {}

    for category, items in data.items():
        if category in keys:
            unique_key = keys[category]
            seen = set()
            unique_list = []

            for item in items:
                identifier = item.get(unique_key)
                if identifier not in seen:
                    seen.add(identifier)
                    unique_list.append(item)

            processed_data[category] = unique_list
        else:
            # If no unique key is specified for a category, return it unchanged
            processed_data[category] = items

    return processed_data


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


def unwrap_nested_dicts(d: dict):
    result = {}
    for key, value in d.items():
        if isinstance(value, dict):
            result.update(value)  # Unwrap the nested dictionary
        else:
            result[key] = value  # Copy the key-value pair if value is not a dictionary
    return result


def convert_string_to_int(value):
    try:
        return int(value)
    except ValueError:
        return value


def convert_dict_values(d: dict):
    # Iterate through each key-value pair in the dictionary
    for key, value in d.items():
        # Convert the value if it is a string
        if isinstance(value, str):
            d[key] = convert_string_to_int(value)
    return d


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


def stringify_values_in_dicts(dict_list: list):
    """
    This function takes a list of dictionaries and converts each value in the dictionaries to a string.

    :param dict_list: List of dictionaries where values need to be converted to strings.
    :return: None; the function modifies the list of dictionaries in-place.
    """
    for dct in dict_list:
        for key in dct.keys():
            dct[key] = str(dct[key])

    return dict_list


def flatten_document(doc: dict, parent_key: str = '', sep: str = '.', max_depth: int | None = None, current_depth: int = 0) -> dict:
    items = {}
    if max_depth is not None and current_depth >= max_depth:
        return {parent_key: type(doc).__name__} if parent_key else {"": type(doc).__name__}

    for k, v in doc.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_document(v, new_key, sep, max_depth, current_depth + 1))
        elif isinstance(v, list) and v and isinstance(v[0], dict):
            items.update(flatten_document(v[0], new_key, sep, max_depth, current_depth + 1))
        else:
            items[new_key] = type(v).__name__
    return items

def schema_signature(doc: dict, max_depth: Optional[int] = 1) -> str:
    """
    Generate a hash representing the schema of a Firestore-style document.
    Supports nested dicts and lists of dicts, up to a specified depth.

    :param doc: Document as dict
    :param max_depth: Maximum depth to explore. None = full depth.
    :return: Hash string of the flattened schema
    """

    def flatten(obj, prefix='', level=0):
        if isinstance(obj, dict) and (max_depth is None or level < max_depth):
            for k, v in obj.items():
                path = f"{prefix}.{k}" if prefix else k
                yield from flatten(v, path, level + 1)

        elif isinstance(obj, list) and (max_depth is None or level < max_depth):
            path = prefix or "[]"
            # If list contains dicts, inspect one layer deeper (all elements assumed to have same structure)
            if obj and all(isinstance(i, dict) for i in obj):
                for k, v in obj[0].items():  # Use first element to infer structure
                    sub_path = f"{path}[].{k}"
                    yield from flatten(v, sub_path, level + 2)
            else:
                yield (path, 'list')

        else:
            yield (prefix, type(obj).__name__ if obj is not None else 'NoneType')

    flat_schema = dict(flatten(doc))
    schema_str = json.dumps(flat_schema, sort_keys=True)
    return hashlib.md5(schema_str.encode()).hexdigest()


def notify_slack(message: str):
    message = {
        "text": message
    }
    response = requests.post(settings.slack_web_hook_url, json=message)

    if response.status_code != 200:
        raise Exception(f"Slack notification failed: {response.text}")
