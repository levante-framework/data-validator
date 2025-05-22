from google.cloud import firestore
from google.oauth2 import service_account
from google.cloud.firestore_v1.base_query import FieldFilter

import pytz
from copy import deepcopy

import utils
from utils import *
import settings
import warnings

warnings.filterwarnings("ignore", message="Detected filter using positional arguments")
logging.basicConfig(level=logging.INFO)

pst_timezone = pytz.timezone('America/Los_Angeles')

default_start_date = datetime(2024, 1, 1)
default_end_date = datetime(2050, 1, 1)


def stringify_variables(variable):
    if isinstance(variable, (dict, list, tuple, bool, str, float, int)):
        return str(variable)
    elif variable is None or variable == 'nan':
        return ""
    else:
        return f'Error converting to string: {variable}'


def to_datetime(dt_str, dt_type):
    if dt_str:
        return datetime.strptime(dt_str, '%Y-%m-%d')
    else:
        if dt_type == 'start':
            return default_start_date
        else:
            return default_end_date


def convert_to_integer(variable):
    try:
        return int(variable)
    except Exception as e:
        return None


# Helper function to split the list into chunks of max 30 items
def chunked_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


class _FirestoreServices:
    def __init__(self):

        admin_sa_info = json.loads(os.getenv('ADMIN_SA'))
        assessment_sa_info = json.loads(os.getenv('ASSESSMENT_SA'))

        # Create credentials
        self.admin_credentials = service_account.Credentials.from_service_account_info(admin_sa_info)
        self.assessment_credentials = service_account.Credentials.from_service_account_info(assessment_sa_info)

        # Initialize Firestore clients
        self.admin_db = firestore.Client(credentials=self.admin_credentials, project=admin_sa_info['project_id'])
        self.assessment_db = firestore.Client(credentials=self.assessment_credentials,
                                              project=assessment_sa_info['project_id'])

    def set_logs_to_firebase(self, response, dataset_id):
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

    def get_groups(self, date_filter: utils.DateFilter, group_filter):
        result = []
        try:
            docs = (self.admin_db.collection('groups')
                    .where('createdAt', '>=', to_datetime(date_filter.start_date, 'start'))
                    .where('createdAt', '<=', to_datetime(date_filter.end_date, 'end'))
                    .get())
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                name = doc_dict.get('name', None)
                tags = str(doc_dict.get('tags')) if doc_dict.get('tags', []) else None
                if not group_filter or name in group_filter:
                    doc_dict.update({
                        'group_id': doc.id,
                        'tags': tags
                    })
                    converted_doc_dict = utils.process_doc_dict(doc_dict=doc_dict)
                    result.append(converted_doc_dict)
        except Exception as e:
            logging.error(f"Error in get_groups: {e}")
        return result

    def get_districts(self, lab_id: str):
        # Does not need to be chunked since districts are unique
        try:
            doc = self.admin_db.collection('districts').document(lab_id).get()
            doc_dict = doc.to_dict()
            doc_dict.update({
                'district_id': doc.id,
            })
            # Convert camelCase to snake_case and handle NaN values
            converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
            # Return a list of dictionaries for EntityController functions to loop through
            return [converted_doc_dict]
        except Exception as e:
            logging.error(f"Error in get_districts: {e}")
            return {}

    def get_districts_by_district_name_list(self, date_filter: utils.DateFilter, district_name_list: list = None):
        result = []
        try:
            docs = (self.admin_db.collection('districts')
                    .where('createdAt', '>=', to_datetime(date_filter.start_date, 'start'))
                    .where('createdAt', '<=', to_datetime(date_filter.end_date, 'end'))
                    .get())
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                name = doc_dict.get('name', None)
                if not district_name_list or name in district_name_list:
                    doc_dict.update({
                        'district_id': doc.id,
                    })
                    converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                    result.append(converted_doc_dict)
        except Exception as e:
            print(f"Error in get_districts_by_district_name_list: {e}")
        return result

    def get_schools(self, district_id: str, chunk_size=100):
        last_doc = None
        total_docs = self.admin_db.collection('schools').where('districtId', '==', district_id).get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                query = (self.admin_db.collection('schools')
                         .where('districtId', '==', district_id)
                         .limit(chunk_size))
                if last_doc:
                    query = query.start_after(last_doc)
                docs = query.get()
                if not docs:
                    break

                current_chunk += 1
                logging.info(f"Setting schools... processing chunk {current_chunk} of {total_chunks} school chunks.")
                for doc in docs:
                    doc_dict = doc.to_dict()
                    doc_dict.update({
                        'school_id': doc.id,
                    })
                    # Convert camelCase to snake_case and handle NaN values
                    converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                    yield converted_doc_dict
                last_doc = docs[-1]
            except Exception as e:
                logging.error(f"Error in get_schools: {e}")
                break

    def get_classes(self, district_id: str, chunk_size=100):
        last_doc = None
        total_docs = self.admin_db.collection('classes').where('districtId', '==', district_id).get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                query = (self.admin_db.collection('classes')
                         .where('districtId', '==', district_id)
                         .limit(chunk_size))
                if last_doc:
                    query = query.start_after(last_doc)
                docs = query.get()
                if not docs:
                    break

                current_chunk += 1
                logging.info(f"Setting classes... processing chunk {current_chunk} of {total_chunks} class chunks.")
                for doc in docs:
                    doc_dict = doc.to_dict()
                    doc_dict.update({
                        'class_id': doc.id,
                    })
                    # Convert camelCase to snake_case and handle NaN values
                    converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                    yield converted_doc_dict
                last_doc = docs[-1]
            except Exception as e:
                logging.error(f"Error in get_classes: {e}")
                break

    def get_tasks(self, task_filter: list, chunk_size=100):
        last_doc = None
        base_query = self.assessment_db.collection('tasks')
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
                            'task_id': doc.id
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
        base_query = self.assessment_db.collection('tasks').document(task_id).collection('variants')
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
                            'task_id': task_id
                        })
                        # Convert camelCase to snake_case and handle NaN values
                        converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                        yield converted_doc_dict
                last_doc = docs[-1]
            except Exception as e:
                logging.error(f"Error in get_variants: {e}")
                break

    def get_administrations(self, date_filter: utils.DateFilter, org_filter: utils.OrgFilter, org_ids, chunk_size=100):
        base_query = self.admin_db.collection('administrations')
        # Apply the date range filters
        base_query = base_query.where('dateCreated', '>=', to_datetime(date_filter.start_date, 'start'))
        base_query = base_query.where('dateCreated', '<=', to_datetime(date_filter.end_date, 'end'))
        # Apply the org range filters
        if org_filter.key:
            if org_filter.operator == "array_contains_any":
                base_query = base_query.where(f"{org_filter.key}.minimalOrgs", org_filter.operator, org_ids)
        base_query = base_query.order_by('dateCreated')

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
                        f"Getting administrations... processing chunk {current_chunk} of {total_chunks} administration chunks.")
                    for doc in docs:
                        doc_dict = doc.to_dict()
                        # if org_filter.key and org_filter.operator and org_filter.value:
                        #     if org_filter.org_operator == "array_contains_any":
                        #         org_value_firebase = doc_dict.get("minimalOrgs", {}).get(org_filter.key, [])
                        #         if not org_value_firebase:
                        #             continue  # Skip this document if the filter condition is not met
                        #         elif set(org_value).isdisjoint(org_value_firebase):
                        #             continue
                        doc_dict.update({
                            'administration_id': doc.id,
                        })
                        # Convert camelCase to snake_case and handle NaN values
                        converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                        yield converted_doc_dict
                    last_doc = docs[-1]
                except Exception as e:
                    logging.error(f"Error in get_administrations: {e}")
                    break

        yield from process_docs(query=base_query)

    def get_users(self, is_guest: bool, date_filter: utils.DateFilter, org_filter: utils.OrgFilter, org_ids,
                  user_filter: utils.UserFilter, chunk_size=100):
        date_field = 'lastUpdated'  # 'created' if is_guest else 'createdAt'
        if is_guest:
            collection_name = 'guests'
            base_query = self.assessment_db.collection(collection_name)
        else:
            collection_name = 'users'
            base_query = self.admin_db.collection(collection_name)

        # Apply the date range filters
        base_query = base_query.where(date_field, '>=', to_datetime(date_filter.start_date, 'start'))
        base_query = base_query.where(date_field, '<=', to_datetime(date_filter.start_date, 'end'))
        if org_filter.key:
            if org_filter.operator == "array_contains_any":
                base_query = base_query.where(f"{org_filter.key}.all", org_filter.operator, org_ids)

        base_query = base_query.order_by(date_field)  # direction=firestore.firestore.Query.DESCENDING

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
                            user_type = doc_dict.get('userType', None)
                            assignments_started = doc_dict.get('assignmentsStarted', False)
                            survey_responses = doc.reference.collection('surveyResponses').limit(1).get()
                            if user_type == 'student' and not assignments_started:
                                continue
                            if user_type in ['teacher', 'parent'] and not survey_responses:
                                continue

                        # Check if user filter is being used
                        if user_filter.key:
                            if user_filter.operator == "contains":
                                user_value_firebase = doc_dict.get(user_filter.key, None)
                                if not user_value_firebase:
                                    continue  # Skip this document if the filter condition is not met
                                elif user_filter.value not in user_value_firebase:
                                    continue

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

    def get_runs(self, user_id: str, is_guest: bool = False, chunk_size=100):
        last_doc = None
        collection_name = 'guests' if is_guest else 'users'
        base_query = (self.assessment_db.collection(collection_name).document(user_id)
                      .collection('runs'))
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
        base_query = (self.assessment_db.collection(collection_name).document(user_id)
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
                    doc_dict.update({
                        'trial_id': doc.id,
                        'user_id': user_id,
                        'run_id': run_id,
                        'task_id': task_id,
                    })
                    timestamp = doc_dict.get('serverTimestamp', None)
                    sig = utils.schema_signature(doc_dict)
                    task_dict = trial_key_usage.setdefault(task_id, {})

                    # If this schema has not been seen before, store it
                    if sig not in task_dict:
                        task_dict[sig] = {
                            'trial_doc': deepcopy(doc_dict),
                            'user_id': user_id,
                            'run_id': run_id,
                            'trial_id': doc.id,
                            'serverTimeStamp': timestamp
                        }

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

    def get_surveys(self, user_id: str, user_type: str, date_filter: utils.DateFilter):
        survey_responses = []

        def reformat_responses(data):
            formatted_responses = []

            def process_item(process_key, process_value):
                boolean_response = None
                string_response = None
                numeric_response = None
                # If value is a dictionary, process nested items
                if isinstance(process_value, dict):
                    for sub_key, sub_value in process_value.items():
                        process_item(sub_key, sub_value)
                elif 'intro' not in process_key.lower():
                    boolean_values = {"Yes", "No"}  # Set for quick lookup

                    if isinstance(process_value, (int, str)):
                        if isinstance(process_value, int) or process_value.isdigit():
                            numeric_response = int(process_value) if not isinstance(process_value,
                                                                                    int) else process_value
                        elif process_value in boolean_values:
                            boolean_response = True if process_value == "Yes" else False
                        else:
                            string_response = process_value
                    else:
                        string_response = str(process_value)

                    # Format and add to the list, converting values to integers when possible
                    formatted_responses.append({
                        "question_id": process_key,
                        "boolean_response": boolean_response,
                        "string_response": string_response,
                        "numeric_response": numeric_response,
                    })

            for key, value in data.items():
                process_item(key, value)

            return formatted_responses

        try:
            docs = (self.admin_db.collection('users').document(user_id).collection('surveyResponses')
                    .where('createdAt', '>=', to_datetime(date_filter.start_date, 'start'))
                    .where('createdAt', '<=', to_datetime(date_filter.start_date, 'end'))
                    .get())

            for doc in docs:
                doc_dict = doc.to_dict()

                survey_responses_dict = doc_dict.get('data', {}).get('surveyResponses', {})
                reformated_survey_responses = []
                if not survey_responses_dict:
                    general = doc_dict.get('general', {})
                    specific = doc_dict.get('specific', [])
                    if general:
                        is_complete = general.get('isComplete', None)
                        general_responses = general.get('responses', {})
                        if general_responses:
                            reformatted_g_data = reformat_responses(data=general_responses)
                            for r in reformatted_g_data:
                                r.update({'is_complete': is_complete})
                            reformated_survey_responses.extend(reformatted_g_data)
                    if specific:
                        for s in specific:
                            child_id = s.get('childId', None)
                            is_complete = s.get('isComplete', None)
                            specific_responses = s.get('responses', {})
                            if specific_responses:
                                reformatted_s_data = reformat_responses(data=specific_responses)
                                for r in reformatted_s_data:
                                    r.update({'is_complete': is_complete, 'child_id': child_id})
                                reformated_survey_responses.extend(reformatted_s_data)
                else:
                    reformated_survey_responses = reformat_responses(data=survey_responses_dict)

                # Processing responses:
                for item in reformated_survey_responses:
                    item.update({
                        'survey_response_id': doc.id,
                        'user_id': user_id,
                        'administration_id': doc_dict.get('administrationId', None),
                        'survey_id': user_type if user_type != 'parent' else 'caregiver',
                        'created_at': doc_dict.get('createdAt', None),
                        'updated_at': doc_dict.get('updatedAt', None),
                    })

                survey_responses.extend(reformated_survey_responses)
        except Exception as e:
            print(f"Error in get_survey_responses: {e}, user_id: {user_id}")
        return survey_responses

    def upload_trial_key_variants_to_firestore(self, trial_key_usage, task_id: str):
        if task_id not in trial_key_usage:
            logging.info(f"No trial key variants for task {task_id}")
            return

        trial_keys_ref = self.assessment_db.collection('tasks').document(task_id).collection('trialKeys')

        for schema_hash, record in trial_key_usage[task_id].items():
            record_with_hash = record.copy()
            record_with_hash['schema_hash'] = schema_hash
            trial_keys_ref.add(record_with_hash)


firestore_services = _FirestoreServices()
