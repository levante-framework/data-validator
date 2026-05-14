import json
import logging
import random
import warnings
from copy import deepcopy
from datetime import datetime, timezone
from itertools import islice

import pytz
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter, Or
from google.oauth2 import service_account

import settings
from shared import utils
from shared.secret_services import secret_service
from shared.utils import flatten_document, handle_nan, process_doc_dict

warnings.filterwarnings("ignore", message="Detected filter using positional arguments")
logging.basicConfig(level=logging.INFO)

default_start_date = "2024-01-01"
default_end_date = "2050-01-01"


def stringify_variables(variable):
    if isinstance(variable, (dict, list, tuple, bool, str, float, int)):
        return str(variable)
    elif variable is None or variable == 'nan':
        return ""
    else:
        return f'Error converting to string: {variable}'


def to_datetime(dt_str, dt_type):
    """
    dt_str: 'YYYY-MM-DD' or None
    dt_type: 'start' or 'end'
    """
    if not dt_str:
        dt_str = default_start_date if dt_type == "start" else default_end_date

    # parse as naive date, then make it UTC and clamp to day boundary
    dt = datetime.strptime(dt_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    if dt_type == "start":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        return dt.replace(hour=23, minute=59, second=59, microsecond=999999)


def convert_to_integer(variable):
    try:
        return int(variable)
    except Exception as e:
        return None


# Helper function to split the list into chunks of max 30 items

def chunked(iterable):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, 30))
        if not chunk:
            break
        yield chunk


# ----------------------------------------------------------------------------
# surveyResponses schema classification
# ----------------------------------------------------------------------------
# Three live shapes in production (confirmed via full scan May 2026):
#   - legacy_data:              {"data": {"surveyResponses": {q: a, ...}}, ...}
#   - legacy_general_specific:  {"general": {...}, "specific": [...], ...}
#   - run_like:                 {"taskId": "child-survey", "timeStarted", ...}
#                               PLUS a `trials` subcollection holding answers.
#                               Per the May 2026 rollout, only "child-survey"
#                               ships in this shape; parent/teacher surveys
#                               stay on legacy_general_specific.
# One intentionally-skipped shape:
#   - pageNo_marker:            draft/state row, no responses (parent autosave)
SURVEY_SCHEMA_LEGACY_DATA = "legacy_data"
SURVEY_SCHEMA_LEGACY_GENERAL_SPECIFIC = "legacy_general_specific"
SURVEY_SCHEMA_RUN_LIKE = "task_run"
SURVEY_SCHEMA_PAGENO_MARKER = "pageNo_marker"
SURVEY_SCHEMA_UNKNOWN = "unknown"

# The single task_id we expect to see in the run-like shape. Anything else
# gets flagged via validation_msg_survey for the curator to investigate.
RUN_LIKE_EXPECTED_TASK_ID = "child-survey"
RUN_LIKE_EXPECTED_SURVEY_TYPE = "student"


def classify_survey_doc(doc_dict: dict) -> str:
    """Label a surveyResponses Firestore doc by its top-level shape."""
    keys = set(doc_dict.keys())
    if "taskId" in keys and ("timeStarted" in keys or "timeFinished" in keys):
        return SURVEY_SCHEMA_RUN_LIKE
    if "general" in keys or "specific" in keys:
        return SURVEY_SCHEMA_LEGACY_GENERAL_SPECIFIC
    if isinstance(doc_dict.get("data"), dict) and "surveyResponses" in doc_dict["data"]:
        return SURVEY_SCHEMA_LEGACY_DATA
    if set(keys) <= {"administrationId", "pageNo", "createdAt", "updatedAt"}:
        return SURVEY_SCHEMA_PAGENO_MARKER
    return SURVEY_SCHEMA_UNKNOWN


def normalize_user_type_to_survey_type(user_type: str) -> str:
    """Match the convention used elsewhere in this file (parent → caregiver)."""
    return "caregiver" if user_type == "parent" else user_type


class FirestoreServices:
    def __init__(self):
        self._admin_db = None
        self._admin_credentials = None

    @property
    def admin_credentials(self):
        if self._admin_credentials is None:
            info = json.loads(
                secret_service.get_secret_payload(secret_id=settings.config['ADMIN_SERVICE_ACCOUNT_SECRET_ID']))
            self._admin_credentials = service_account.Credentials.from_service_account_info(info)
        return self._admin_credentials


    @property
    def admin_db(self):
        if self._admin_db is None:
            self._admin_db = firestore.Client(credentials=self.admin_credentials,
                                              project=self.admin_credentials.project_id)
        return self._admin_db


    def set_logs_to_firebase(self, response, dataset_id):
        pst_timezone = pytz.timezone('America/Los_Angeles')
        date_doc_name = datetime.now(pst_timezone).strftime("%Y-%m-%d")
        datetime_doc_name = datetime.now(pst_timezone).strftime("%Y-%m-%d %H:%M:%S")
        try:
            # Adding document to the 'logs' collection with an auto-generated ID
            doc_ref = (self.admin_db.collection('logs')
                       .document(dataset_id)
                       .collection(date_doc_name)
                       .document(datetime_doc_name))
            doc_ref.set(response)
            logging.info(f'Document written with ID: {doc_ref.id}')
        except Exception as e:
            logging.info(f'An error occurred: {e}')

    def get_district_name(self, district_id: str) -> str | None:
        """Human-readable name from `districts/{district_id}` (site == district)."""
        try:
            doc = self.admin_db.collection("districts").document(district_id).get()
            if not doc.exists:
                return None
            return (doc.to_dict() or {}).get("name")
        except Exception as e:
            logging.error(f"get_district_name({district_id!r}): {e}")
            return None

    def find_district_id_by_name(self, name: str) -> str | None:
        """
        Return the document id for `districts` where ``name`` or ``normalizedName``
        equals the given string (trimmed). None if not found or if more than one
        document matches on a field.
        """
        name = (name or "").strip()
        if not name:
            return None
        try:
            col = self.admin_db.collection("districts")
            for field in ("name", "normalizedName"):
                docs = list(
                    col.where(filter=FieldFilter(field, "==", name)).limit(2).get()
                )
                if len(docs) == 1:
                    return docs[0].id
                if len(docs) > 1:
                    logging.warning(
                        "find_district_id_by_name(%r): multiple districts match on %s; skipping",
                        name,
                        field,
                    )
                    return None
            return None
        except Exception as e:
            logging.error("find_district_id_by_name(%r): %s", name, e)
            return None

    def get_org_by_org_id_list(self, org_name: str, org_id_list: list):
        result = []
        if org_name == "site":
            org_in_firebase = "districts"
        elif org_name == "cohort":
            org_in_firebase = "groups"
        elif org_name == "school":
            org_in_firebase = "schools"
        elif org_name == "class":
            org_in_firebase = "classes"
        else:
            return
        try:
            for org_id in org_id_list:
                doc_ref = self.admin_db.collection(org_in_firebase).document(org_id)
                doc = doc_ref.get()
                if doc.exists:
                    doc_dict = doc.to_dict()

                    doc_dict.update({
                        f'{org_name}_id': doc.id,
                        f'{org_name}_name': doc_dict.get('name', None),
                        f'{org_name}_abbreviation': doc_dict.get('abbreviation', None),
                    })
                    converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                    result.append(converted_doc_dict)

        except Exception as e:
            print(f"Error in get_org_by_org_id_list: {e}")
        return result

    def get_tasks(self, task_filter: list, chunk_size=100):
        last_doc = None
        base_query = self.admin_db.collection('tasks')
        total_docs = base_query.get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0

        while True:
            try:
                query = base_query.limit(chunk_size)
                if last_doc:
                    query = query.start_after(last_doc)
                docs = query.get()
                if not docs:
                    break

                current_chunk += 1
                logging.info(f"Setting tasks... processing chunk {current_chunk} of {total_chunks} chunks.")
                for doc in docs:
                    doc_dict = doc.to_dict()  # Convert the document to a dictionary
                    if not task_filter or doc.id in task_filter:
                        doc_dict.update({
                            'task_id': doc.id,
                            'task_name': doc_dict.get('name', None),
                        })
                        # Convert camelCase to snake_case and handle NaN values
                        converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                        yield converted_doc_dict
                last_doc = docs[-1]
            except Exception as e:
                logging.error(f"Error in get_tasks: {e}")
                break

    def get_variants(self, task_id: str, variant_filter: list, chunk_size=100):
        last_doc = None
        base_query = self.admin_db.collection('tasks').document(task_id).collection('variants')
        total_docs = base_query.get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                query = base_query.limit(chunk_size)
                if last_doc:
                    query = query.start_after(last_doc)
                docs = query.get()
                if not docs:
                    break
                current_chunk += 1
                logging.info(f"Setting variants... processing chunk {current_chunk} of {total_chunks} "
                             f"chunks for task {task_id}.")
                for doc in docs:
                    doc_dict = doc.to_dict()
                    param_dict = doc_dict.get('params', {})
                    if not variant_filter or doc.id in variant_filter:
                        doc_dict.update(param_dict)
                        doc_dict.update({
                            'variant_id': doc.id,
                            'task_id': task_id,
                            'variant_name': doc_dict.get('name', None),
                        })
                        # Convert camelCase to snake_case and handle NaN values
                        converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                        yield converted_doc_dict
                last_doc = docs[-1]
            except Exception as e:
                logging.error(f"Error in get_variants: {e}")
                break

    def get_administrations_by_ids(self, administration_ids, chunk_size: int = 100):
        try:
            if not administration_ids:
                return []

            col = self.admin_db.collection('administrations')
            results = []

            for i in range(0, len(administration_ids), chunk_size):
                chunk = administration_ids[i:i + chunk_size]
                try:
                    doc_refs = [col.document(doc_id) for doc_id in chunk]
                    # get_all returns a generator of DocumentSnapshot
                    docs = list(self.admin_db.get_all(doc_refs))
                    for snap in docs:
                        if not snap.exists:
                            continue
                        d = snap.to_dict() or {}
                        d['administration_id'] = snap.id
                        d['administration_name'] = d.get('name', None)
                        converted_doc_dict = process_doc_dict(doc_dict=d)
                        results.append(converted_doc_dict)
                except Exception as e:
                    logging.error(f"[get_administrations_by_ids] chunk {i}-{i + len(chunk)} failed: {e}", exc_info=True)
                    continue

            return results
        except Exception as e:
            logging.error(f"[get_administrations_by_ids] fatal: {e}", exc_info=True)
            return []

    def iter_administrations_for_site(self, site_id: str):
        """
        Stream administration documents for a given Firestore site id (field: siteId).
        Yields raw dicts including administration_id.
        """
        try:
            for snap in self.admin_db.collection("administrations").where(
                "siteId", "==", site_id
            ).stream():
                if not snap.exists:
                    continue
                d = snap.to_dict() or {}
                d["administration_id"] = snap.id
                yield d
        except Exception as e:
            logging.error(f"iter_administrations_for_site({site_id!r}): {e}", exc_info=True)

    def get_users(
        self,
        is_guest: bool,
        date_filter: utils.DateFilter,
        org_filter: utils.OrgFilter | None,
        user_filter: utils.UserFilter | None,
        user_number_limit: int | None = None,
        chunk_size=100,
    ):
        date_field = 'lastUpdated'  # 'created' if is_guest else 'createdAt'
        if is_guest:
            collection_name = 'guests'
        else:
            collection_name = 'users'

        base_query = self.admin_db.collection(collection_name)
        # Apply the date range filters
        # base_query = base_query.where(date_field, '>=', to_datetime(date_filter.start_date, 'start'))
        # base_query = base_query.where(date_field, '<=', to_datetime(date_filter.end_date, 'end'))
        if org_filter is not None:
            if org_filter.operator == "array_contains_any":
                base_query = base_query.where(f"{org_filter.key}.current", org_filter.operator, org_filter.value)

        if user_filter is not None:
            if user_filter.operator == "starts_with" and user_filter.value:
                prefix_field = user_filter.key
                prefix = user_filter.value
                base_query = base_query.where(prefix_field, '>=', prefix)
                base_query = base_query.where(prefix_field, '<', prefix + u'\uf8ff')
                base_query = base_query.order_by(prefix_field)

        base_query = base_query.order_by(date_field)  # direction=firestore.firestore.Query.DESCENDING

        # def _parse_iso_z(s: str):
        #     try:
        #         return datetime.fromisoformat(s.replace('Z', '+00:00'))
        #     except Exception:
        #         return None

        def _any_assigned_between(assigned_map: dict, start_dt, end_dt) -> bool:
            """
            Returns True if ANY assignmentsAssigned entry has a __time__ within [start_dt, end_dt].
            Supports values like {"__time__": "...Z"} or raw ISO strings.
            """
            if not isinstance(assigned_map, dict):
                return False
            for administration_id, assigned_time in assigned_map.items():
                if assigned_time is not None and start_dt <= assigned_time <= end_dt:
                    return True
            return False

        def _has_activity(doc_ref, doc_dict: dict) -> bool:
            start_dt = to_datetime(date_filter.start_date, 'start')
            end_dt = to_datetime(date_filter.end_date, 'end')

            # administrations: assignment exists in filtered date range
            has_admin = _any_assigned_between(doc_dict.get('assignmentsAssigned', {}), start_dt, end_dt)

            # runs in filtered date range
            runs = (doc_ref.collection('runs')
                    .where('timeStarted', '>=', start_dt)
                    .where('timeStarted', '<=', end_dt)
                    .limit(1).get())
            has_runs = bool(runs)

            # surveys in filtered date range
            surveys = (doc_ref.collection('surveyResponses')
                       .where('createdAt', '>=', start_dt)
                       .where('createdAt', '<=', end_dt)
                       .limit(1).get())
            has_surveys = bool(surveys)

            return has_admin or has_runs or has_surveys

        def _normalize_user_doc(user_id: str, doc_dict: dict) -> dict:
            doc_dict = dict(doc_dict or {})
            doc_dict['user_id'] = user_id
            doc_dict['birth_year'] = convert_to_integer(doc_dict.get('birthYear', None))
            doc_dict['birth_month'] = convert_to_integer(doc_dict.get('birthMonth', None))

            parent_ids = doc_dict.get('parentIds', [])
            teacher_ids = doc_dict.get('teacherIds', [])
            grade = doc_dict.get('grade', None)
            doc_dict['teacher_id'] = teacher_ids[0] if teacher_ids else None
            doc_dict['parent1_id'] = parent_ids[0] if parent_ids else None
            doc_dict['parent2_id'] = parent_ids[1] if parent_ids and len(parent_ids) == 2 else None
            doc_dict['grade'] = grade if doc_dict.get('grade', None) else None

            if doc_dict.get('created', None):
                doc_dict['created_at'] = doc_dict.get('created')
            return process_doc_dict(doc_dict=doc_dict)

        def _extract_related_user_ids(doc_dict: dict) -> set[str]:
            related_ids: set[str] = set()
            for key in ("parentIds", "teacherIds", "childIds"):
                raw_ids = doc_dict.get(key, [])
                if isinstance(raw_ids, list):
                    for uid in raw_ids:
                        if isinstance(uid, str) and uid:
                            related_ids.add(uid)
            return related_ids

        def process_docs(query):
            last_doc = None
            total_docs = query.get()
            total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
            current_chunk = 0
            selected_by_id: dict[str, dict] = {}
            related_user_ids: set[str] = set()

            while True:
                try:
                    query = query.limit(chunk_size)
                    if last_doc:
                        query = query.start_after(last_doc)
                    docs = query.get()
                    if not docs:
                        break
                    current_chunk += 1
                    logging.info(
                        f"Setting users... processing chunk {current_chunk} of {total_chunks} {collection_name} chunks.")
                    for doc in docs:
                        doc_dict = doc.to_dict()
                        # Discard users without any assignment.
                        if is_guest:
                            runs = doc.reference.collection('runs').limit(1).get()
                            if not runs:  # Check if there are no documents in the runs subcollection
                                continue
                        else:
                            if not _any_assigned_between(
                                    doc_dict.get('assignmentsAssigned', {}),
                                    to_datetime(date_filter.start_date, 'start'),
                                    to_datetime(date_filter.end_date, 'end')
                            ):
                                continue
                            user_type = doc_dict.get('userType', None)
                            assignments_started = doc_dict.get('assignmentsStarted', False)
                            survey_responses = doc.reference.collection('surveyResponses').limit(1).get()
                            if user_type == 'student' and not assignments_started:
                                continue
                            if user_type in ['teacher', 'parent'] and not survey_responses:
                                continue
                            if user_type in ['admin']:
                                continue

                        # Check if user filter is being used
                        # if user_filter.key:
                        #     if user_filter.operator == "contains":
                        #         user_value_firebase = doc_dict.get(user_filter.key, None)
                        #         if not user_value_firebase:
                        #             continue  # Skip this document if the filter condition is not met
                        #         elif user_filter.value not in user_value_firebase:
                        #             continue

                        converted_doc_dict = _normalize_user_doc(user_id=doc.id, doc_dict=doc_dict)
                        related_user_ids.update(_extract_related_user_ids(doc_dict))
                        if user_number_limit and user_number_limit > 0:
                            if _has_activity(doc.reference, doc_dict):
                                selected_by_id[converted_doc_dict["user_id"]] = converted_doc_dict
                        else:
                            selected_by_id[converted_doc_dict["user_id"]] = converted_doc_dict
                    last_doc = docs[-1]
                except Exception as e:
                    logging.error(f"Error in get_users: {e}")
                    break

            selected_users = list(selected_by_id.values())
            if user_number_limit and user_number_limit > 0 and len(selected_users) > user_number_limit:
                selected_users = random.sample(selected_users, user_number_limit)
                selected_by_id = {u["user_id"]: u for u in selected_users}

            # Backfill one-hop relationship-linked users even if they have no runs/surveys/admin activity.
            missing_related_ids = related_user_ids - set(selected_by_id.keys())
            if missing_related_ids:
                rel_collection = self.admin_db.collection('users')
                missing_related_ids = list(missing_related_ids)
                random.shuffle(missing_related_ids)
                rel_refs = [rel_collection.document(uid) for uid in missing_related_ids]
                for snap in self.admin_db.get_all(rel_refs):
                    if not snap.exists:
                        continue
                    if user_number_limit and user_number_limit > 0 and len(selected_by_id) >= user_number_limit:
                        break
                    rel_doc = snap.to_dict() or {}
                    normalized = _normalize_user_doc(user_id=snap.id, doc_dict=rel_doc)
                    selected_by_id[normalized["user_id"]] = normalized

            for user in selected_by_id.values():
                yield user

        yield from process_docs(query=base_query)

    def get_runs(self, user_id: str, run_key_usage: dict, date_filter: utils.DateFilter, is_guest: bool = False,
                 chunk_size=100):
        last_doc = None
        collection_name = 'guests' if is_guest else 'users'
        base_query = (self.admin_db.collection(collection_name).document(user_id)
                      .collection('runs'))
        base_query = base_query.where('timeStarted', '>=', to_datetime(date_filter.start_date, 'start'))
        base_query = base_query.where('timeStarted', '<=', to_datetime(date_filter.end_date, 'end'))
        base_query = base_query.order_by('timeStarted')
        total_docs = base_query.get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0

        while True:
            try:
                query = base_query.limit(chunk_size)
                if last_doc:
                    query = query.start_after(last_doc)
                docs = query.get()
                if not docs:
                    break

                current_chunk += 1
                logging.info(f"Setting runs... processing chunk {current_chunk} of {total_chunks} "
                             f"run chunks for {collection_name} {user_id}.")
                for doc in docs:
                    doc_dict = doc.to_dict()
                    test_comp_scores = doc_dict.get('scores', {}).get('raw', {}).get('composite', {}).get('test', {})
                    time_created = doc_dict.get("timeStarted", None)  # or handle __time__ if needed
                    task_id = doc_dict.get('taskId', None)
                    task_version = doc_dict.get('taskVersion', None)

                    flattened = flatten_document(doc_dict, max_depth=None)
                    task_dict = run_key_usage.setdefault(task_id, {})

                    for key, value in flattened.items():
                        new_meta = {
                            "user_id": user_id,
                            "run_id": doc.id,
                            "task_version": task_version,
                            "time_created": time_created
                        }

                        if key not in task_dict:
                            task_dict[key] = new_meta
                        else:
                            prev_time = task_dict[key].get("time_created")
                            if prev_time is None or (time_created and time_created > prev_time):
                                task_dict[key] = new_meta

                    doc_dict.update({
                        'run_id': doc.id,
                        'user_id': user_id,
                        'administration_id': doc_dict.get('assignmentId', None),
                        'num_attempted': test_comp_scores.get('numAttempted', None),
                        'num_correct': test_comp_scores.get('numCorrect', None),
                        'test_comp_theta_estimate': test_comp_scores.get('thetaEstimate', None),
                        'test_comp_theta_se': test_comp_scores.get('thetaSE', None)
                    })
                    # Convert camelCase to snake_case and handle NaN values
                    converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                    yield converted_doc_dict
                last_doc = docs[-1]
            except Exception as e:
                logging.error(f"Error in get_runs: {e}")
                break

    def get_trials(self, user_id: str, run_id: str, task_id: str, trial_key_usage: dict, is_guest: bool = False,
                   chunk_size=100):
        last_doc = None
        collection_name = 'guests' if is_guest else 'users'
        base_query = (self.admin_db.collection(collection_name).document(user_id)
                      .collection('runs').document(run_id)
                      .collection('trials'))
        total_docs = base_query.get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                query = base_query.limit(chunk_size)
                if last_doc:
                    query = query.start_after(last_doc)
                docs = query.get()
                if not docs:
                    break

                current_chunk += 1
                logging.info(
                    f"Setting trials... processing chunk {current_chunk} of {total_chunks} "
                    f"trial chunks of run {run_id} for user {user_id}.")
                for doc in docs:
                    doc_dict = doc.to_dict()
                    time_created = doc_dict.get('serverTimestamp', None)

                    flattened = flatten_document(doc_dict, max_depth=1)
                    task_dict = trial_key_usage.setdefault(task_id, {})

                    for key, value in flattened.items():
                        new_meta = {
                            "user_id": user_id,
                            "run_id": run_id,
                            "trial_id": doc.id,
                            "time_created": time_created,
                        }

                        if key not in task_dict:
                            task_dict[key] = new_meta
                        else:
                            prev_time = task_dict[key].get("time_created")
                            if prev_time is None or (time_created and time_created > prev_time):
                                task_dict[key] = new_meta

                    doc_dict.update({
                        'trial_id': doc.id,
                        'user_id': user_id,
                        'run_id': run_id,
                        'task_id': task_id,
                    })

                    # Add identifiers to the dictionary
                    if settings.config['INSTANCE'] == 'ROAR':
                        doc_dict.update({
                            # Pop required Firekit attributes
                            'correct': handle_nan(doc_dict.pop('correct', None)),
                            'assessment_stage': handle_nan(doc_dict.pop('assessment_stage', None)),
                            # Pop default jsPsych data attributes
                            'trial_index': handle_nan(doc_dict.pop('trial_index', None)),
                            'trial_type': handle_nan(doc_dict.pop('trial_type', None)),
                            'time_elapsed': handle_nan(doc_dict.pop('time_elapsed', None)),
                        })

                        # Ignore keys which we do not want duplicated in trial_attributes
                        ignore_keys = ['trial_id', 'user_id', 'run_id', 'task_id']
                        # Process the remaining doc_dict keys
                        doc_dict['trial_attributes'] = process_doc_dict(doc_dict, ignore_keys)
                        converted_doc_dict = doc_dict
                    else:
                        answer = doc_dict.get(
                            'answer',
                            doc_dict.get('goal', doc_dict.get('sequence', doc_dict.get('word', None)))
                        )
                        item = doc_dict.get('item')
                        distractors = doc_dict.get('distractors')
                        subtask = doc_dict.get('subtask')
                        response = doc_dict.get('response')
                        response_location = doc_dict.get('responseLocation')
                        rt = doc_dict.get('rt')
                        doc_dict.update({
                            # 'corpus_trial_type': stringify_variables(doc_dict.get('corpus_trial_type', '')),
                            'item': stringify_variables(item) if item is not None else None,
                            'distractors': stringify_variables(distractors) if distractors is not None else None,
                            'answer': stringify_variables(answer) if answer is not None else None,
                            'subtask': stringify_variables(subtask) if subtask is not None else None,
                            'response': stringify_variables(response) if response is not None else None,
                            'rt': rt if isinstance(rt, int) else (stringify_variables(rt) if rt is not None else None),
                            'response_location': stringify_variables(response_location) if response_location is not None else None,
                        })
                        converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                    yield converted_doc_dict
                last_doc = docs[-1]
            except Exception as e:
                logging.error(f"Error in get_trails: {e}")
                break

    def get_surveys(self, user_id: str, user_type: str, date_filter: utils.DateFilter, survey_key_usage: dict):
        surveys: list[dict] = []
        survey_responses: list[dict] = []

        def reformat_responses(data):
            formatted_responses = []

            def is_response_object(d):
                return isinstance(d, dict) and ("responseValue" in d or "responseTime" in d)

            def extract_response_value(d):
                return d.get("responseValue", None) if isinstance(d, dict) else d

            def extract_response_time(d):
                if not isinstance(d, dict):
                    return None
                rt = d.get("responseTime")
                if isinstance(rt, dict) and "__time__" in rt:
                    return rt.get("__time__")
                return rt

            def process_item(process_key, process_value, inherited_response_time=None):
                # Skip intro questions
                if "intro" in (process_key or "").lower():
                    return

                response_time = inherited_response_time

                # New response-object shape -> extract time + value
                if is_response_object(process_value):
                    rt = extract_response_time(process_value)
                    if rt is not None:
                        response_time = rt
                    process_value = extract_response_value(process_value)

                # If responseValue is a dict: recurse BUT keep the same response_time
                if isinstance(process_value, dict):
                    for sub_key, sub_value in process_value.items():
                        process_item(sub_key, sub_value, response_time)
                    return

                # If responseValue is a list (e.g. TeacherGrad): store as JSON string (stable)
                if isinstance(process_value, list):
                    response = json.dumps(process_value, ensure_ascii=False)
                    response_type = "string"
                else:
                    if process_value is None:
                        response = None
                        response_type = None
                    elif isinstance(process_value, bool):
                        response = process_value
                        response_type = "boolean"
                    elif isinstance(process_value, (int, float)):
                        response = process_value
                        response_type = "numeric"
                    elif isinstance(process_value, str):
                        response = process_value
                        response_type = "string"
                    else:
                        response = str(process_value)
                        response_type = "string"

                formatted_responses.append({
                    "question_id": process_key,
                    "response": response,
                    "response_type": response_type,
                    "response_time": response_time,
                })

            for key, value in (data or {}).items():
                process_item(key, value, None)

            return formatted_responses

        def make_survey_id(doc_id: str, scope: str, child_id: str | None = None) -> str:
            # Ensure uniqueness across general vs per-child specific sections
            if child_id:
                return f"{doc_id}:{scope}:{child_id}"
            return f"{doc_id}:{scope}"

        survey_type = normalize_user_type_to_survey_type(user_type)

        try:
            sr_collection = (
                self.admin_db.collection('users').document(user_id).collection('surveyResponses')
            )
            start_dt = to_datetime(date_filter.start_date, 'start')
            end_dt = to_datetime(date_filter.end_date, 'end')

            # Two queries unioned by doc id. The first catches the three legacy
            # shapes (all keyed on createdAt); the second catches the run-like
            # shape which has no createdAt — only timeStarted.
            docs_by_created = list(
                sr_collection.where('createdAt', '>=', start_dt)
                             .where('createdAt', '<=', end_dt)
                             .get()
            )
            try:
                docs_by_started = list(
                    sr_collection.where('timeStarted', '>=', start_dt)
                                 .where('timeStarted', '<=', end_dt)
                                 .get()
                )
            except Exception as e:
                # timeStarted index may not exist yet; treat as empty rather than
                # crashing the entire user's survey ingest.
                logging.warning(
                    "get_surveys: timeStarted query failed for user_id=%s (%s); "
                    "run-like docs may be missed until the index is built.",
                    user_id, e,
                )
                docs_by_started = []

            seen_ids: set[str] = set()
            docs = []
            for d in (*docs_by_created, *docs_by_started):
                if d.id in seen_ids:
                    continue
                seen_ids.add(d.id)
                docs.append(d)

            for doc in docs:
                doc_dict = doc.to_dict() or {}
                schema = classify_survey_doc(doc_dict)

                if schema == SURVEY_SCHEMA_PAGENO_MARKER:
                    logging.info(
                        "get_surveys: skipping pageNo marker user_id=%s doc_id=%s",
                        user_id, doc.id,
                    )
                    continue

                if schema == SURVEY_SCHEMA_UNKNOWN:
                    logging.warning(
                        "get_surveys: unknown surveyResponses shape; "
                        "user_id=%s doc_id=%s keys=%s",
                        user_id, doc.id, sorted(doc_dict.keys()),
                    )
                    continue

                # ---------------- key usage tracking ----------------
                # Run-like docs flatten into the same task_dict; that's fine —
                # it tracks any key Firestore is sending us.
                time_created_for_keys = (
                    doc_dict.get('createdAt') or doc_dict.get('timeStarted')
                )
                flattened = flatten_document(doc=doc_dict, max_depth=2)
                task_dict = survey_key_usage.setdefault(f'{user_type}_survey', {})
                for key, value in flattened.items():
                    new_meta = {
                        "user_id": user_id,
                        "survey_response_id": doc.id,
                        "time_created": time_created_for_keys,
                    }
                    if key not in task_dict:
                        task_dict[key] = new_meta
                    else:
                        prev_time = task_dict[key].get("time_created")
                        if prev_time is None or (
                            time_created_for_keys and time_created_for_keys > prev_time
                        ):
                            task_dict[key] = new_meta

                # ---------------- branch on schema ----------------
                if schema == SURVEY_SCHEMA_RUN_LIKE:
                    s_row, r_rows = self._parse_run_like_survey(
                        doc=doc, doc_dict=doc_dict,
                        user_id=user_id, user_type=user_type,
                        fallback_survey_type=survey_type,
                        make_survey_id=make_survey_id,
                    )
                    surveys.append(s_row)
                    survey_responses.extend(r_rows)
                    continue

                # Legacy shapes (data / general / specific) share the same emit
                # logic — figure out which scope(s) apply, then iterate.
                doc_created_at = doc_dict.get('createdAt')
                doc_updated_at = doc_dict.get('updatedAt')
                administration_id = doc_dict.get('administrationId')

                survey_instances: list[dict] = []
                if schema == SURVEY_SCHEMA_LEGACY_DATA:
                    legacy_flat = (doc_dict.get('data') or {}).get('surveyResponses') or {}
                    if legacy_flat:
                        survey_instances.append({
                            "scope": "data",
                            "child_id": None,
                            "is_complete": doc_dict.get("isComplete"),
                            "responses": legacy_flat,
                        })
                elif schema == SURVEY_SCHEMA_LEGACY_GENERAL_SPECIFIC:
                    general = doc_dict.get('general') or {}
                    specific = doc_dict.get('specific') or []
                    if isinstance(general, dict) and general.get('responses'):
                        survey_instances.append({
                            "scope": "general",
                            "child_id": None,
                            "is_complete": general.get('isComplete'),
                            "responses": general.get('responses', {}),
                        })
                    if isinstance(specific, list):
                        for s in specific:
                            if not isinstance(s, dict) or not s.get('responses'):
                                continue
                            survey_instances.append({
                                "scope": "specific",
                                "child_id": s.get('childId'),
                                "is_complete": s.get('isComplete'),
                                "responses": s.get('responses', {}),
                            })

                for inst in survey_instances:
                    scope = inst["scope"]
                    child_id = inst.get("child_id")
                    is_complete = inst.get("is_complete")
                    responses_map = inst.get("responses") or {}

                    survey_part = "specific" if scope == "specific" else "general"
                    sid = make_survey_id(doc.id, scope, child_id)

                    surveys.append({
                        "survey_id": sid,
                        "administration_id": administration_id,
                        "user_id": user_id,
                        "child_id": child_id,
                        "survey_type": survey_type,
                        "survey_part": survey_part,
                        "is_complete": is_complete,
                        "created_at": doc_created_at,
                        "updated_at": doc_updated_at,
                        "survey_schema_source": schema,
                    })

                    for item in reformat_responses(data=responses_map):
                        effective_response_time = (
                            item.get("response_time") or doc_created_at
                        )
                        survey_responses.append({
                            "survey_id": sid,
                            "question": item.get("question_id"),
                            "survey_part": survey_part,
                            "survey_type": survey_type,
                            "response": item.get("response"),
                            "response_type": item.get("response_type"),
                            "timestamp": effective_response_time,
                            "survey_schema_source": schema,
                        })

        except Exception as e:
            logging.error(
                "Error in get_surveys user_id=%s: %s", user_id, e, exc_info=True,
            )

        return surveys, survey_responses

    def _parse_run_like_survey(
        self,
        *,
        doc,
        doc_dict: dict,
        user_id: str,
        user_type: str,
        fallback_survey_type: str,
        make_survey_id,
    ) -> tuple[dict, list[dict]]:
        """
        Emit one ``Survey`` row + N ``SurveyResponse`` rows for a run-like
        surveyResponses doc. Trials are read from the doc's ``trials``
        subcollection; the audio file is used as the question identifier per
        product decision (until a stable question id ships on the trial doc).
        """
        task_id = doc_dict.get('taskId')
        time_started = doc_dict.get('timeStarted')
        time_finished = doc_dict.get('timeFinished')
        admin_id = doc_dict.get('assignmentId') or doc_dict.get('administrationId')

        # Per the May 2026 rollout, the only run-like task is "child-survey"
        # which always maps to survey_type=student. Anything else is an
        # anomaly worth flagging — keep the row but raise it via
        # validation_msg_survey.
        normalized_user_type = normalize_user_type_to_survey_type(user_type)
        if task_id == RUN_LIKE_EXPECTED_TASK_ID:
            survey_type = RUN_LIKE_EXPECTED_SURVEY_TYPE
            if (
                normalized_user_type
                and normalized_user_type != survey_type
            ):
                validation_msg = (
                    f"survey_type_mismatch(task_id={task_id},"
                    f"expected={survey_type},user_type={normalized_user_type})"
                )
            else:
                validation_msg = None
        else:
            survey_type = normalized_user_type
            validation_msg = f"unexpected_run_like_task_id({task_id!r})"

        # Per the May 2026 rollout, child-survey is always tagged "general"
        # in the survey metadata. survey_id uses the canonical
        # {doc_id}:{survey_part}[:child_id] design — the schema variant
        # (task_run vs legacy_*) is tracked separately on survey_schema_source.
        run_like_survey_part = "general"
        sid = make_survey_id(doc.id, scope=run_like_survey_part, child_id=None)
        survey_row = {
            "survey_id": sid,
            "administration_id": admin_id,
            "user_id": user_id,
            "child_id": None,
            "survey_type": survey_type,
            "survey_part": run_like_survey_part,
            "is_complete": bool(doc_dict.get('completed')),
            "created_at": time_started,
            "updated_at": time_finished,
            "survey_schema_source": SURVEY_SCHEMA_RUN_LIKE,
            "validation_msg_survey": validation_msg,
        }

        response_rows: list[dict] = []
        try:
            trial_snaps = list(doc.reference.collection('trials').get())
        except Exception as e:
            logging.error(
                "get_surveys: failed to fetch trials for run-like survey "
                "user_id=%s doc_id=%s: %s",
                user_id, doc.id, e,
            )
            trial_snaps = []

        for trial_snap in trial_snaps:
            t = trial_snap.to_dict() or {}
            # Skip practice and instruction trials — both are non-data-bearing.
            if t.get('isPracticeTrial'):
                continue
            if str(t.get('assessment_stage', '')).strip().lower() == 'instructions':
                continue
            question = t.get('audioFile')
            if not question:
                # No stable identifier on a non-instruction, non-practice trial.
                # Worth a debug log so we can spot data-quality gaps.
                logging.debug(
                    "get_surveys: run-like trial without audioFile; "
                    "user_id=%s survey_id=%s trial_id=%s",
                    user_id, sid, trial_snap.id,
                )
                continue
            # Numeric responses come from `responseLocation` (encoded 0..N).
            # The `answer` field is reserved for future string/boolean responses
            # — for now it's just a localized label of the numeric value.
            response_rows.append({
                "survey_id": sid,
                "question": question,
                "survey_part": run_like_survey_part,
                "survey_type": survey_type,
                "response": t.get('responseLocation'),
                "response_type": "numeric",
                "timestamp": t.get('serverTimestamp') or t.get('createdAt') or time_started,
                "survey_schema_source": SURVEY_SCHEMA_RUN_LIKE,
            })

        return survey_row, response_rows

    def upload_task_schema_to_firestore(self, dict_type: str, schema_usage: dict, task_id: str,
                                        new_schemas: list):
        if task_id not in schema_usage and task_id != "survey":
            return

        task_doc_ref = self.admin_db.collection('tasks').document(task_id)
        task_doc = task_doc_ref.get().to_dict() or {}
        stored_dict = task_doc.get(dict_type, {})

        task_type = dict_type if task_id == "survey" else task_id

        updated = False
        is_new_data_fields = False
        for key, local_meta in schema_usage[task_type].items():
            if key not in stored_dict:
                stored_dict[key] = local_meta
                updated = True
                is_new_data_fields = True
            else:
                existing_ts = stored_dict[key].get("time_created")
                new_ts = local_meta.get("time_created")
                if new_ts and (existing_ts is None or new_ts > existing_ts):
                    stored_dict[key] = local_meta
                    updated = True

        if updated:
            task_doc_ref.update({dict_type: stored_dict})
            msg = f"New {dict_type} to {task_doc_ref.id}.{task_type}"
            if is_new_data_fields:
                new_schemas.append(msg)


firestore_services = FirestoreServices()

# def get_administrations(self, group_ids, district_ids, school_ids, chunk_size=100):
#     base = self.admin_db.collection('administrations')
#
#     def process_docs(query):
#         last_doc = None
#         while True:
#             try:
#                 q = query.limit(chunk_size)
#                 if last_doc:
#                     q = q.start_after(last_doc)
#                 docs = q.get()
#                 if not docs:
#                     break
#                 for doc in docs:
#                     d = doc.to_dict()
#                     d.update({'administration_id': doc.id})
#                     yield process_doc_dict(doc_dict=d)
#                 last_doc = docs[-1]
#             except Exception as e:
#                 logging.error(f"Error in get_administrations: {e}")
#                 break
#         # Build per-field chunked queries (<=30 values each)
#
#     queries = []
#
#     if group_ids:
#         for chunk in chunked(group_ids):
#             queries.append(base.where(filter=FieldFilter("minimalOrgs.groups", "array_contains_any", chunk)))
#
#     if district_ids:
#         for chunk in chunked(district_ids):
#             queries.append(base.where(filter=FieldFilter("minimalOrgs.districts", "array_contains_any", chunk)))
#
#     if school_ids:
#         for chunk in chunked(school_ids):
#             queries.append(base.where(filter=FieldFilter("minimalOrgs.schools", "array_contains_any", chunk)))
#
#     # If no org filters provided, run the base query once
#     if not queries:
#         queries = [base]
#
#     # Run each query and de-dupe by doc id
#     seen = set()
#     for q in queries:
#         for row in process_docs(q):
#             doc_id = row.get('administration_id')
#             if doc_id in seen:
#                 continue
#             seen.add(doc_id)
#             yield row
# def get_groups_by_district_ids(self, district_ids: list, chunk_size=100):
#     def process_docs(query):
#         last_doc = None
#         total_docs = query.get()
#         total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
#         current_chunk = 0
#         while True:
#             try:
#                 query = query.limit(chunk_size)
#                 if last_doc:
#                     query = query.start_after(last_doc)
#                 docs = query.get()
#                 if not docs:
#                     break
#                 current_chunk += 1
#                 logging.info(f"Setting groups... processing chunk {current_chunk} of {total_chunks} groups chunks.")
#                 for doc in docs:
#                     doc_dict = doc.to_dict()
#                     tags = str(doc_dict.get('tags')) if doc_dict.get('tags', []) else None
#                     doc_dict.update({
#                         'group_id': doc.id,
#                         'tags': tags
#                     })
#                     # Convert camelCase to snake_case and handle NaN values
#                     converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
#                     yield converted_doc_dict
#                 last_doc = docs[-1]
#             except Exception as e:
#                 logging.error(f"Error in get_groups_by_district_id for {query}: {e}")
#                 break
#
#     for district_id in district_ids:
#         base_query = self.admin_db.collection('groups').where('parentOrgId', '==', district_id)
#         yield from process_docs(base_query)
#
#
# def get_schools_by_district_ids(self, district_ids: list, chunk_size=100):
#     def process_docs(query):
#         last_doc = None
#         total_docs = query.get()
#         total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
#         current_chunk = 0
#         while True:
#             try:
#                 query = query.limit(chunk_size)
#                 if last_doc:
#                     query = query.start_after(last_doc)
#                 docs = query.get()
#                 if not docs:
#                     break
#                 current_chunk += 1
#                 logging.info(
#                     f"Setting schools... processing chunk {current_chunk} of {total_chunks} school chunks.")
#                 for doc in docs:
#                     doc_dict = doc.to_dict()
#                     doc_dict.update({
#                         'school_id': doc.id,
#                     })
#                     # Convert camelCase to snake_case and handle NaN values
#                     converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
#                     yield converted_doc_dict
#                 last_doc = docs[-1]
#             except Exception as e:
#                 logging.error(f"Error in get_schools_by_district_ids for {query}: {e}")
#                 break
#
#     for district_id in district_ids:
#         base_query = self.admin_db.collection('schools').where('districtId', '==', district_id)
#         yield from process_docs(base_query)
#
#
# def get_classes_by_school_ids(self, school_ids: list, chunk_size=100):
#     def process_docs(query):
#         last_doc = None
#         total_docs = query.get()
#         total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
#         current_chunk = 0
#         while True:
#             try:
#                 query = query.limit(chunk_size)
#                 if last_doc:
#                     query = query.start_after(last_doc)
#                 docs = query.get()
#                 if not docs:
#                     break
#                 current_chunk += 1
#                 logging.info(f"Setting classes... processing chunk {current_chunk} of {total_chunks} class chunks.")
#                 for doc in docs:
#                     doc_dict = doc.to_dict()
#                     doc_dict.update({
#                         'class_id': doc.id,
#                     })
#                     # Convert camelCase to snake_case and handle NaN values
#                     converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
#                     yield converted_doc_dict
#                 last_doc = docs[-1]
#             except Exception as e:
#                 logging.error(f"Error in get_classes_by_school_ids: {e}")
#                 break
#
#     for school_id in school_ids:
#         base_query = (self.admin_db.collection('classes')
#                       .where('schoolId', '==', school_id)
#                       )
#         yield from process_docs(base_query)
# def get_districts_by_district_name_list(self, district_name_list: list):
#     result = []
#     try:
#         docs = self.admin_db.collection('districts').get()
#         for doc in docs:
#             doc_dict = doc.to_dict()  # Convert the document to a dictionary
#             name = doc_dict.get('name', None)
#             if name in district_name_list or not district_name_list:
#                 doc_dict.update({
#                     'district_id': doc.id,
#                 })
#                 converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
#                 result.append(converted_doc_dict)
#     except Exception as e:
#         print(f"Error in get_districts_by_district_name_list: {e}")
#     return result
#
#
# def get_groups_by_group_names(self, group_names_list: list):
#     result = []
#     try:
#         docs = self.admin_db.collection('groups').get()
#
#         for doc in docs:
#             doc_dict = doc.to_dict()  # Convert the document to a dictionary
#             name = doc_dict.get('name', None)
#             tags = str(doc_dict.get('tags')) if doc_dict.get('tags', []) else None
#             if name in group_names_list:
#                 doc_dict.update({
#                     'group_id': doc.id,
#                     'tags': tags
#                 })
#                 converted_doc_dict = utils.process_doc_dict(doc_dict=doc_dict)
#                 result.append(converted_doc_dict)
#
#     except Exception as e:
#         logging.error(f"Error in get_groups: {e}")
#     return result
