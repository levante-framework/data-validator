import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os
import logging
from datetime import datetime
import json
from utils import process_doc_dict, handle_nan
from secret_services import secret_services
import settings

logging.basicConfig(level=logging.INFO)


def stringify_variables(variable):
    if isinstance(variable, (dict, list, tuple, int, float, bool, str)):
        return str(variable)
    elif variable is None or variable == 'nan':
        return None
    else:
        return f'Error converting to string: {variable}'


# Helper function to split the list into chunks of max 30 items
def chunked_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


class FirestoreServices:
    default_app = None
    db = None

    def __init__(self, app_name, start_date, end_date):
        try:
            # Check if the app already exists
            self.default_app = firebase_admin.get_app(name=app_name)
        except ValueError:
            # If the app does not exist, initialize it based on the app_name
            if app_name == 'assessment_site':
                assessment_cred = secret_services.access_secret_version(
                    secret_id=settings.config['ASSESSMENT_SERVICE_ACCOUNT_SECRET_ID'],
                    version_id="latest")
                cred = credentials.Certificate(json.loads(assessment_cred))
            else:
                cred = credentials.ApplicationDefault()

            self.default_app = firebase_admin.initialize_app(credential=cred, name=app_name)
            self.db = firestore.client(self.default_app)

            self.start_date = (datetime.strptime(start_date, "%m/%d/%Y")
                               .replace(hour=0, minute=0, second=0, microsecond=0)) if start_date else datetime(2024, 1,
                                                                                                                1)
            self.end_date = (datetime.strptime(end_date, '%m/%d/%Y')
                             .replace(hour=23, minute=59, second=59, microsecond=999999)) if end_date else datetime(
                2050, 1, 1)
        except Exception as e:
            logging.info(f"Error in {app_name} FirestoreService init: {e}")

    def get_groups(self, lab_id: str):
        # Does not need to be chunked since groups are unique
        try:
            doc = self.db.collection('groups').document(lab_id).get()
            doc_dict = doc.to_dict()
            doc_dict.update({
                'group_id': doc.id,
            })
            # Convert camelCase to snake_case and handle NaN values
            converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
            # Return a list of dictionaries for EntityController functions to loop through
            return [converted_doc_dict]
        except Exception as e:
            logging.error(f"Error in get_groups: {e}")
            return {}

    def get_groups_by_group_name_list(self, group_name_list: list):
        result = []
        try:
            docs = (self.db.collection('groups')
                    .where('createdAt', '>=', self.start_date)
                    .where('createdAt', '<=', self.end_date)
                    .get())
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                name = doc_dict.get('name', None)
                if not group_name_list or name in group_name_list:
                    doc_dict.update({
                        'group_id': doc.id,
                    })
                    converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                    result.append(converted_doc_dict)

        except Exception as e:
            print(f"Error in get_groups_by_group_name_list: {e}")
        return result

    def get_districts(self, lab_id: str):
        # Does not need to be chunked since districts are unique
        try:
            doc = self.db.collection('districts').document(lab_id).get()
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

    def get_districts_by_district_name_list(self, district_name_list: list = None):
        result = []
        try:
            docs = (self.db.collection('districts')
                    .where('createdAt', '>=', self.start_date)
                    .where('createdAt', '<=', self.end_date)
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

    def get_schools(self, lab_id: str, chunk_size=100):
        last_doc = None
        total_docs = self.db.collection('schools').where('districtId', '==', lab_id).get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                query = (self.db.collection('schools')
                         .where('districtId', '==', lab_id)
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

    def get_classes(self, lab_id: str, chunk_size=100):
        last_doc = None
        total_docs = self.db.collection('classes').where('districtId', '==', lab_id).get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                query = (self.db.collection('classes')
                         .where('districtId', '==', lab_id)
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

    def get_tasks(self, chunk_size=100):
        last_doc = None
        base_query = self.db.collection('tasks')
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
                    doc_dict = doc.to_dict()
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

    def get_variants(self, task_id: str, chunk_size=100):
        last_doc = None
        base_query = self.db.collection('tasks').document(task_id).collection('variants')
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

    def get_assignments(self, filter_list: list, filter_by: str, chunk_size=100):
        base_query = self.db.collection('administrations')

        # Apply the date range filters
        base_query = base_query.where('dateCreated', '>=', self.start_date)
        base_query = base_query.where('dateCreated', '<=', self.end_date)
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
                        f"Setting assignments... processing chunk {current_chunk} of {total_chunks} assignment chunks.")
                    for doc in docs:
                        doc_dict = doc.to_dict()
                        doc_dict.update({
                            'assignment_id': doc.id,
                        })
                        # Convert camelCase to snake_case and handle NaN values
                        converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                        yield converted_doc_dict
                    last_doc = docs[-1]
                except Exception as e:
                    logging.error(f"Error in get_users: {e}")
                    break

        if filter_by:
            org_chunks = chunked_list(filter_list, 30)
            # Iterate over each chunk of organization IDs
            for org_chunk in org_chunks:
                print("Processing org chunk:", org_chunk)  # Print the current chunk
                filtered_query = base_query.where(f'minimalOrgs.{filter_by}', 'array_contains_any', org_chunk)
                yield from process_docs(query=filtered_query)
        else:
            yield from process_docs(query=base_query)

    def get_users(self, filter_list: list, filter_by: str, chunk_size=100):
        collection_name = 'guests' if os.environ.get('guest_mode') else 'users'
        date_field = 'created' if os.environ.get('guest_mode') else 'lastUpdated'
        filter_field = None if os.environ.get('guest_mode') or filter_by is None else f'{filter_by}.current'
        base_query = self.db.collection(collection_name)

        # Apply the date range filters
        base_query = base_query.where(date_field, '>=', self.start_date)
        base_query = base_query.where(date_field, '<=', self.end_date)
        base_query = base_query.order_by(date_field)

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
                    logging.info(f"Setting users... processing chunk {current_chunk} of {total_chunks} user chunks.")
                    for doc in docs:
                        doc_dict = doc.to_dict()
                        doc_dict.update({
                            'user_id': doc.id,
                        })
                        # Convert camelCase to snake_case and handle NaN values
                        converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                        yield converted_doc_dict
                    last_doc = docs[-1]
                except Exception as e:
                    logging.error(f"Error in get_users: {e}")
                    break

        if filter_field:
            org_chunks = chunked_list(filter_list, 30)
            # Iterate over each chunk of organization IDs
            for org_chunk in org_chunks:
                print("Processing org chunk:", org_chunk)  # Print the current chunk
                filtered_query = base_query.where(filter_field, 'array_contains_any', org_chunk)
                yield from process_docs(query=filtered_query)
        else:
            yield from process_docs(query=base_query)

    def get_runs(self, user_id: str, chunk_size=100):
        last_doc = None
        collection_name = 'guests' if os.environ.get('guest_mode') else 'users'
        base_query = (self.db.collection(collection_name).document(user_id)
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
                             f"run chunks for user {user_id}.")
                for doc in docs:
                    doc_dict = doc.to_dict()
                    test_comp_scores = doc_dict.get('scores', {}).get('raw', {}).get('composite', {}).get('test', {})

                    doc_dict.update({
                        'run_id': doc.id,
                        'user_id': user_id,
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

    def get_trials(self, user_id: str, run_id: str, task_id: str, chunk_size=100):
        last_doc = None
        collection_name = 'guests' if os.environ.get('guest_mode') else 'users'
        base_query = (self.db.collection(collection_name).document(user_id)
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
                        doc_dict.update({
                            'item': stringify_variables(doc_dict['item']) if doc_dict.get('item') is not None else None,
                            'distractors': stringify_variables(doc_dict['distractors']) if doc_dict.get(
                                'distractors') is not None else None,
                            'answer': answer if doc_dict.get('answer') is not None else None,
                            'response': stringify_variables(doc_dict.get('response', None)),
                            'rt': stringify_variables(doc_dict['rt']) if doc_dict.get('rt') is not None else None
                        })

                        converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                    yield converted_doc_dict
                last_doc = docs[-1]
            except Exception as e:
                logging.error(f"Error in get_trails: {e}")
                break

    def get_survey_responses(self, user_id: str):
        result = []
        try:
            docs = (self.db.collection('users').document(user_id).collection('surveyResponses')
                    .where('createdAt', '>=', self.start_date)
                    .where('createdAt', '<=', self.end_date)
                    .get())

            for doc in docs:
                doc_dict = doc.to_dict()
                survey_responses_dict = doc_dict.get('data', {}).get('surveyResponses', {})
                doc_dict['survey_response_id'] = doc.id
                doc_dict['user_id'] = user_id
                doc_dict['created_at'] = doc_dict.get('createdAt', None)
                doc_dict['class_friends'] = survey_responses_dict.get('ClassFriends', None)
                doc_dict['class_help'] = survey_responses_dict.get('ClassHelp', None)
                doc_dict['class_nice'] = survey_responses_dict.get('ClassNice', None)
                doc_dict['class_play'] = survey_responses_dict.get('ClassPlay', None)
                doc_dict['example1_comic'] = survey_responses_dict.get('Example1Comic', None)
                doc_dict['example2_neat'] = survey_responses_dict.get('Example2Neat', None)
                doc_dict['growth_mind_math'] = survey_responses_dict.get('GrowthMindMath', None)
                doc_dict['growth_mind_read'] = survey_responses_dict.get('GrowthMindRead', None)
                doc_dict['growth_mind_smart'] = survey_responses_dict.get('GrowthMindSmart', None)
                doc_dict['learning_good'] = survey_responses_dict.get('LearningGood', None)
                doc_dict['lonely_school'] = survey_responses_dict.get('LonelySchool', None)
                doc_dict['math_enjoy'] = survey_responses_dict.get('MathEnjoy', None)
                doc_dict['math_good'] = survey_responses_dict.get('MathGood', None)
                doc_dict['reading_enjoy'] = survey_responses_dict.get('ReadingEnjoy', None)
                doc_dict['reading_good'] = survey_responses_dict.get('ReadingGood', None)
                doc_dict['school_enjoy'] = survey_responses_dict.get('SchoolEnjoy', None)
                doc_dict['school_fun'] = survey_responses_dict.get('SchoolFun', None)
                doc_dict['school_give_up'] = survey_responses_dict.get('SchoolGiveUp', None)
                doc_dict['school_happy'] = survey_responses_dict.get('SchoolHappy', None)
                doc_dict['school_safe'] = survey_responses_dict.get('SchoolSafe', None)
                doc_dict['teacher_like'] = survey_responses_dict.get('TeacherLike', None)
                doc_dict['teacher_listen'] = survey_responses_dict.get('TeacherListen', None)
                doc_dict['teacher_nice'] = survey_responses_dict.get('TeacherNice', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_survey_responses: {e}")
        return result

