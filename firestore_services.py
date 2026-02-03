from google.cloud import firestore
from google.oauth2 import service_account
from secret_services import secret_service
from google.cloud.firestore_v1.base_query import FieldFilter, Or
import pytz
from itertools import islice
from copy import deepcopy

import utils
from utils import *
import settings
import warnings

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

    def get_users(self, is_guest: bool, date_filter: utils.DateFilter, org_filter: utils.OrgFilter,
                  user_filter: utils.UserFilter, chunk_size=100):
        date_field = 'lastUpdated'  # 'created' if is_guest else 'createdAt'
        if is_guest:
            collection_name = 'guests'
        else:
            collection_name = 'users'

        base_query = self.admin_db.collection(collection_name)
        # Apply the date range filters
        # base_query = base_query.where(date_field, '>=', to_datetime(date_filter.start_date, 'start'))
        # base_query = base_query.where(date_field, '<=', to_datetime(date_filter.end_date, 'end'))
        if org_filter.key:
            if org_filter.operator == "array_contains_any":
                base_query = base_query.where(f"{org_filter.key}.current", org_filter.operator, org_filter.value)

        if user_filter.key:
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

        def process_docs(query):
            last_doc = None
            total_docs = query.get()
            total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
            current_chunk = 0

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

                        doc_dict['user_id'] = doc.id
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
                        # Convert camelCase to snake_case and handle NaN values
                        converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                        yield converted_doc_dict
                    last_doc = docs[-1]
                except Exception as e:
                    logging.error(f"Error in get_users: {e}")
                    break

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
                        answer = doc_dict.get('answer', doc_dict.get('sequence', doc_dict.get('word', None)))
                        rt = doc_dict.get('rt', '')
                        doc_dict.update({
                            # 'corpus_trial_type': stringify_variables(doc_dict.get('corpus_trial_type', '')),
                            'item': stringify_variables(doc_dict.get('item', '')),
                            'distractors': stringify_variables(doc_dict.get('distractors', '')),
                            'answer': stringify_variables(answer) if answer is not None else "",
                            'response': stringify_variables(doc_dict.get('response', '')),
                            'rt': rt if isinstance(rt, int) else stringify_variables(rt),
                            'response_location': stringify_variables(doc_dict.get('responseLocation', '')),
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

            def coerce_answer(v):
                boolean_response = None
                string_response = None
                numeric_response = None

                if v is None:
                    return boolean_response, string_response, numeric_response
                if isinstance(v, bool):
                    return v, None, None
                if isinstance(v, int):
                    return None, None, v
                if isinstance(v, str):
                    s = v.strip()
                    if s.lower() in ("yes", "no"):
                        return (s.lower() == "yes"), None, None
                    try:
                        return None, None, int(s)
                    except Exception:
                        return None, s, None
                return None, str(v), None

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
                    boolean_response, string_response, numeric_response = None, json.dumps(process_value,
                                                                                           ensure_ascii=False), None
                else:
                    boolean_response, string_response, numeric_response = coerce_answer(process_value)

                formatted_responses.append({
                    "question_id": process_key,
                    "boolean_response": boolean_response,
                    "string_response": string_response,
                    "numeric_response": numeric_response,
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

        survey_type = user_type if user_type != "parent" else "caregiver"

        try:
            docs = (self.admin_db.collection('users').document(user_id).collection('surveyResponses')
                    .where('createdAt', '>=', to_datetime(date_filter.start_date, 'start'))
                    .where('createdAt', '<=', to_datetime(date_filter.end_date, 'end'))
                    .order_by('createdAt')
                    .get())

            for doc in docs:
                doc_dict = doc.to_dict()
                time_created = doc_dict.get('createdAt', None)

                # key usage tracking (unchanged)
                flattened = flatten_document(doc=doc_dict, max_depth=2)
                task_dict = survey_key_usage.setdefault(f'{user_type}_survey', {})

                for key, value in flattened.items():
                    new_meta = {
                        "user_id": user_id,
                        "survey_response_id": doc.id,
                        "time_created": time_created,
                    }
                    if key not in task_dict:
                        task_dict[key] = new_meta
                    else:
                        prev_time = task_dict[key].get("time_created")
                        if prev_time is None or (time_created and time_created > prev_time):
                            task_dict[key] = new_meta

                doc_created_at = doc_dict.get('createdAt', None)
                doc_updated_at = doc_dict.get('updatedAt', None)
                administration_id = doc_dict.get('administrationId', None)

                survey_instances: list[dict] = []

                # 1) legacy: data.surveyResponses
                legacy_flat = doc_dict.get('data', {}).get('surveyResponses', {})
                if legacy_flat:
                    survey_instances.append({
                        "scope": "data",
                        "child_id": None,
                        "is_complete": doc_dict.get("isComplete", None),
                        "responses": legacy_flat,
                    })
                else:
                    # 2) legacy: general/specific
                    general = doc_dict.get('general', {})
                    specific = doc_dict.get('specific', [])

                    if general and isinstance(general, dict) and general.get('responses'):
                        survey_instances.append({
                            "scope": "general",
                            "child_id": None,
                            "is_complete": general.get('isComplete', None),
                            "responses": general.get('responses', {}),
                        })

                    if specific and isinstance(specific, list):
                        for s in specific:
                            if not isinstance(s, dict):
                                continue
                            if not s.get('responses'):
                                continue
                            survey_instances.append({
                                "scope": "specific",
                                "child_id": s.get('childId', None),
                                "is_complete": s.get('isComplete', None),
                                "responses": s.get('responses', {}),
                            })

                    # 3) newer: root-level {isComplete, responses}
                    if not survey_instances:
                        root_responses = doc_dict.get('responses', None)
                        if root_responses:
                            survey_instances.append({
                                "scope": "root",
                                "child_id": None,
                                "is_complete": doc_dict.get('isComplete', None),
                                "responses": root_responses,
                            })

                for inst in survey_instances:
                    scope = inst["scope"]  # "data" | "general" | "specific" | "root"
                    child_id = inst.get("child_id")
                    is_complete = inst.get("is_complete")
                    responses_map = inst.get("responses", {}) or {}

                    # survey_part: only "specific" is specific; everything else is general
                    survey_part = "specific" if scope == "specific" else "general"

                    sid = make_survey_id(doc.id, scope, child_id)

                    surveys.append({
                        "survey_id": sid,
                        "administration_id": administration_id,
                        "user_id": user_id,
                        "child_id": child_id,
                        "survey_type": survey_type,
                        "survey_part": survey_part,  # <-- NEW
                        "is_complete": is_complete,
                        "created_at": doc_created_at,
                        "updated_at": doc_updated_at,
                    })

                    # survey_responses rows
                    reformatted = reformat_responses(data=responses_map)
                    for item in reformatted:
                        effective_response_time = item.get("response_time") or doc_created_at
                        survey_responses.append({
                            "survey_id": sid,
                            "question": item.get("question_id"),
                            "boolean_response": item.get("boolean_response"),
                            "string_response": item.get("string_response"),
                            "numeric_response": item.get("numeric_response"),
                            "timestamp": effective_response_time,
                        })

        except Exception as e:
            print(f"Error in get_survey_responses: {e}, user_id: {user_id}")

        return surveys, survey_responses

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
