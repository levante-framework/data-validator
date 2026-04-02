from __future__ import annotations

import os
import re
import hashlib
import math
import settings
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List
from dotenv import load_dotenv
import json
import requests

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Union, Optional, Literal, get_args, get_origin
from datetime import datetime
import hashlib, base64, re

ID_FIELDS = {
    # core user-bearing tables
    "users": ["user_id", "parent1_id", "parent2_id", "teacher_id"],
    "runs": ["user_id"],
    "trials": ["user_id"],
    "surveys": ["user_id", "child_id"],

    # survey_responses currently has no user_id column
    "survey_responses": [],

    # join tables
    "user_administrations": ["user_id"],
    "user_sites": ["user_id"],
    "user_cohorts": ["user_id"],
    "user_schools": ["user_id"],
    "user_classes": ["user_id"],

    # legacy alias kept for backward compatibility if present in any older exports
    "user_groups": ["user_id"],
}


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

    @model_validator(mode='after')
    def check_date_range(self):
        if self.start_date and self.end_date:
            start = datetime.strptime(self.start_date, '%Y-%m-%d')
            end = datetime.strptime(self.end_date, '%Y-%m-%d')
            if start > end:
                raise ValueError("start_date must be less than or equal to end_date")
        return self


class UserFilter(BaseModel):
    key: Optional[str] = Field(default=None)
    operator: Optional[str] = Field(default=None, pattern="^(starts_with|<=|>=|==)$")
    value: Optional[Union[int, str]] = Field(default=None)

    @model_validator(mode='before')
    def validate_value(cls, data):
        if not isinstance(data, dict):
            return data
        operator = data.get('operator')
        value = data.get('value')
        if operator:
            if operator == 'starts_with' and not isinstance(value, str):
                raise TypeError("Value must be a string when operator is 'starts_with'")
            elif operator in ['<=', '>=', '=='] and not isinstance(value, int):
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
        if v is None:
            return v
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
    is_user_id_masked: bool = Field(default=False)
    user_number_limit: Optional[int] = Field(default=None, ge=1)
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
            org_names = org.filters.org_filter.value if org.filters.org_filter.value else 'None'
            date_range = f"{org.filters.date_filter.start_date} to {org.filters.date_filter.end_date}" if org.filters.date_filter.start_date and org.filters.date_filter.end_date else "No date limit"
            org_summary.append(
                f"{org.org_id} ({'guests' if org.is_guest else 'users'}): Org ({org_names}), Date range: {date_range}, User limit: {org.user_number_limit}")

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
        if len(value) == 0:
            return None
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


def flatten_document(doc: dict, parent_key: str = '', sep: str = '.', max_depth: int | None = None,
                     current_depth: int = 0) -> dict:
    items = {}
    if max_depth is not None and current_depth >= max_depth:
        return {parent_key: type(doc).__name__} if parent_key else {"": type(doc).__name__}

    for k, v in doc.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_document(v, new_key, sep, max_depth, current_depth + 1))
        elif isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
            for elem in v:
                items.update(flatten_document(elem, new_key, sep, max_depth, current_depth + 1))
        else:
            items[new_key] = type(v).__name__
    return items


def schema_registry():
    """
        Map export table name -> (controller list attribute, model class).
        Adjust to match exactly the tables you want in Redivis.
        """
    import core_models
    # pick concrete user/run/trial classes based on INSTANCE
    use_levante = settings.config.get("INSTANCE") == "LEVANTE"
    UserCls = core_models.LevanteUser if use_levante else core_models.UserBase
    RunCls = core_models.LevanteRun if use_levante else core_models.RunBase
    TrialCls = core_models.LevanteTrial if use_levante else core_models.TrialBase

    return {
        # org dimensions
        "sites": ("valid_sites", getattr(core_models, "SiteBase")),
        "cohorts": ("valid_cohorts", getattr(core_models, "CohortBase")),
        "schools": ("valid_schools", getattr(core_models, "SchoolBase")),
        "classes": ("valid_classes", getattr(core_models, "ClassBase")),

        # core dimensions
        "administrations": ("valid_administrations", core_models.AdministrationBase),
        "tasks": ("valid_tasks", core_models.TaskBase),
        "variants": ("valid_variants", core_models.VariantBase),

        # facts
        "users": ("valid_users", UserCls),
        "runs": ("valid_runs", RunCls),
        "trials": ("valid_trials", TrialCls),
        "surveys": ("valid_surveys", core_models.Survey),
        "survey_responses": ("valid_survey_responses", core_models.SurveyResponse),

        # joins
        "user_administrations": ("valid_user_administrations", core_models.UserAdministration),
        "user_sites": ("valid_user_sites", core_models.UserSite),
        "user_cohorts": ("valid_user_cohorts", core_models.UserCohort),
        "user_schools": ("valid_user_schools", core_models.UserSchool),
        "user_classes": ("valid_user_classes", core_models.UserClass),
    }


def _sentinel_from_annotation(ann: Any, now: datetime) -> Any:
    origin = get_origin(ann)
    base = ann

    # Unwrap Optional/Union[..., None]
    if origin is Union:
        args = [a for a in get_args(ann) if a is not type(None)]
        base = args[0] if args else Any
        origin = get_origin(base)

    if base is str:
        return "schema_row"
    if base is int:
        return 0
    if base is float:
        return 0.0001
    if base is bool:
        return False
    if base is datetime:
        return now
    if origin in (list, set, tuple):
        return []
    if origin is dict:
        return {}

    return "schema_row"


def append_schema_rows_to_validated_data(validated_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    For every table in validated_data that is a non-empty list of rows (list[dict]),
    append a single 'schema row' inferred from existing columns/types.
    Idempotent-by-convention: call this ONCE per export pipeline.
    """

    out: Dict[str, Any] = {}
    reg = schema_registry()
    now = datetime.now(timezone.utc)

    for t_name in reg.keys():
        validated_data.setdefault(t_name, [])

    for table, rows in validated_data.items():
        # leave invalid_data unchanged
        if table == "invalid_data":
            out[table] = rows
            continue

        # get model class from registry entry (table -> (attr, model_cls))
        entry = reg.get(table)
        model_cls = entry[1]
        fields = getattr(model_cls, "model_fields", {}) or {}

        # Build a typed schema row strictly from model annotations
        schema_row = {name: _sentinel_from_annotation(f.annotation, now)
                      for name, f in fields.items()}

        # Always append exactly one schema row
        if isinstance(rows, list):
            out[table] = list(rows) + [schema_row]
        else:
            # Coerce non-list payloads into a single-row list with the schema row
            out[table] = [schema_row]

    return out


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
    from secret_services import secret_service
    slack_web_hook_url = secret_service.get_secret_payload(secret_id=settings.config['SLACK_NOTIFICATION_WEB_HOOK'])
    response = requests.post(slack_web_hook_url, json=message)

    if response.status_code != 200:
        raise Exception(f"Slack notification failed: {response.text}")


def make_id_pseudonymizer(secret_salt: str):
    cache = {}

    def pseudonymize(raw: str) -> str:
        if raw is None:
            return None
        if raw in cache:
            return cache[raw]
        # deterministic, same length & [A-Za-z0-9_-] friendly
        h = hashlib.blake2b((raw + secret_salt).encode("utf-8"), digest_size=24).digest()
        b32 = base64.b32encode(h).decode("utf-8").rstrip("=")  # uppercase A-Z2-7
        # Trim/shape to roughly match original length/charset; fall back to 16 if too short
        target_len = max(16, len(raw))
        fake = b32[:target_len]
        cache[raw] = fake
        return fake

    return pseudonymize


def pseudonymize_dataset(data: dict, salt: str) -> dict:
    pseudo = make_id_pseudonymizer(salt)
    out = {}
    for table, rows in data.items():
        if not isinstance(rows, list):
            out[table] = rows  # e.g., invalid_data blob
            continue
        fields = ID_FIELDS.get(table, [])
        new_rows = []
        for row in rows:
            r = dict(row)
            for f in fields:
                if f in r:
                    r[f] = pseudo(r[f])
            new_rows.append(r)
        out[table] = new_rows
    return out


def ids_with_active(org_map):
    """
    Build (id, is_active) for *every* id in org_map['all'].
    is_active is True iff the id also appears in org_map['current'].
    Missing/empty 'current' -> all ids are False.
    """
    if not isinstance(org_map, dict):
        return []

    # Collect lists, tolerate non-list values
    all_ids_raw = org_map.get('all') or []
    current_raw = org_map.get('current') or []

    if not isinstance(all_ids_raw, (list, tuple, set)):
        all_ids_raw = [all_ids_raw] if all_ids_raw else []
    if not isinstance(current_raw, (list, tuple, set)):
        current_raw = [current_raw] if current_raw else []

    # De-dup ALL while preserving original order; keep only truthy strings
    all_ids = []
    seen = set()
    for x in all_ids_raw:
        x = (x or "").strip() if isinstance(x, str) else x
        if x and x not in seen:
            seen.add(x)
            all_ids.append(x)

    # Current set (membership test only)
    current_set = set(
        (c or "").strip() if isinstance(c, str) else c
        for c in current_raw if c
    )

    return [(oid, oid in current_set) for oid in all_ids]
