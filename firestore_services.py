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

    def get_all_groups(self):
        result = []
        try:
            docs = self.db.collection('groups').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict.update({
                    'group_id': doc.id,
                })
                converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                result.append(converted_doc_dict)

        except Exception as e:
            print(f"Error in get_groups_by_group_id_list: {e}")
        return result

    def get_groups_by_group_name_list(self, group_ids: list):
        result = []
        try:
            docs = self.db.collection('groups').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                name = doc_dict.get('name', None)
                if name in group_ids:
                    doc_dict.update({
                        'group_id': doc.id,
                    })
                    converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                    result.append(converted_doc_dict)
        except Exception as e:
            print(f"Error in get_groups_by_group_id_list: {e}")
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

    def get_districts_by_district_id_list(self, district_ids: list):
        result = []
        try:
            docs = self.db.collection('districts').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                if doc.id in district_ids:
                    doc_dict.update({
                        'district_id': doc.id,
                    })
                    converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                    result.append(converted_doc_dict)
        except Exception as e:
            print(f"Error in get_districts_by_district_id_list: {e}")
        return result

    def get_schools(self, lab_id: str, chunk_size=100):
        last_doc = None
        total_docs = self.db.collection('schools').where('districtId', '==', lab_id).get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                if last_doc:
                    docs = (self.db.collection('schools')
                            .where('districtId', '==', lab_id)
                            .limit(chunk_size)
                            .start_after(last_doc)
                            .get())
                else:
                    docs = (self.db.collection('schools')
                            .where('districtId', '==', lab_id)
                            .limit(chunk_size)
                            .get())
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
                if last_doc:
                    docs = (self.db.collection('classes')
                            .where('districtId', '==', lab_id)
                            .limit(chunk_size)
                            .start_after(last_doc)
                            .get())
                else:
                    docs = (self.db.collection('classes')
                            .where('districtId', '==', lab_id)
                            .limit(chunk_size)
                            .get())
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

    def get_users(self, lab_id: str, chunk_size=100):
        last_doc = None
        total_docs = (self.db.collection('users')
                      .where('districts.current', 'array_contains', lab_id)
                      .where('lastUpdated', '>=', self.start_date)
                      .where('lastUpdated', '<=', self.end_date)
                      .get())
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                if os.environ.get('guest_mode', None):
                    if last_doc:
                        docs = (self.db.collection('guests')
                                .where('created', '>=', self.start_date)
                                .where('created', '<=', self.end_date)
                                .limit(chunk_size)
                                .start_after(last_doc)
                                .get())
                    else:
                        docs = (self.db.collection('guests')
                                .where('created', '>=', self.start_date)
                                .where('created', '<=', self.end_date)
                                .limit(chunk_size)
                                .get())
                else:
                    if last_doc:
                        docs = (self.db.collection('users')
                                .where('districts.current', 'array_contains', lab_id)
                                .where('lastUpdated', '>=', self.start_date)
                                .where('lastUpdated', '<=', self.end_date)
                                .limit(chunk_size)
                                .start_after(last_doc)
                                .get())
                    else:
                        docs = (self.db.collection('users')
                                .where('districts.current', 'array_contains', lab_id)
                                .where('lastUpdated', '>=', self.start_date)
                                .where('lastUpdated', '<=', self.end_date)
                                .limit(chunk_size)
                                .get())
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

    def get_users_by_group_name_list(self, valid_group_ids: list, chunk_size=100):
        last_doc = None
        total_docs = (self.db.collection('users')
                      .where('groups.current', 'array_contains_any', valid_group_ids)
                      .where('lastUpdated', '>=', self.start_date)
                      .where('lastUpdated', '<=', self.end_date)
                      .get())
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                if os.environ.get('guest_mode', None):
                    if last_doc:
                        docs = (self.db.collection('guests')
                                .where('created', '>=', self.start_date)
                                .where('created', '<=', self.end_date)
                                .limit(chunk_size)
                                .start_after(last_doc)
                                .get())
                    else:
                        docs = (self.db.collection('guests')
                                .where('created', '>=', self.start_date)
                                .where('created', '<=', self.end_date)
                                .limit(chunk_size)
                                .get())
                else:
                    if last_doc:
                        docs = (self.db.collection('users')
                                .where('groups.current', 'array_contains_any', valid_group_ids)
                                .where('lastUpdated', '>=', self.start_date)
                                .where('lastUpdated', '<=', self.end_date)
                                .limit(chunk_size)
                                .start_after(last_doc)
                                .get())
                    else:
                        docs = (self.db.collection('users')
                                .where('groups.current', 'array_contains_any', valid_group_ids)
                                .where('lastUpdated', '>=', self.start_date)
                                .where('lastUpdated', '<=', self.end_date)
                                .limit(chunk_size)
                                .get())
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
                logging.error(f"Error in get_users_by_group_name_list: {e}")
                break

    def get_runs(self, user_id: str, chunk_size=100):
        last_doc = None
        total_docs = self.db.collection('users').document(user_id).collection('runs').get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                if os.environ.get('guest_mode', None):
                    if last_doc:
                        docs = (self.db.collection('guests')
                                .document(user_id)
                                .collection('runs')
                                .limit(chunk_size)
                                .start_after(last_doc)
                                .get())
                    else:
                        docs = (self.db.collection('guests')
                                .document(user_id)
                                .collection('runs')
                                .limit(chunk_size)
                                .get())
                else:
                    if last_doc:
                        docs = (self.db.collection('users')
                                .document(user_id)
                                .collection('runs')
                                .limit(chunk_size)
                                .start_after(last_doc)
                                .get())
                    else:
                        docs = (self.db.collection('users')
                                .document(user_id)
                                .collection('runs')
                                .limit(chunk_size)
                                .get())
                if not docs:
                    break
                current_chunk += 1
                logging.info(f"Setting runs... processing chunk {current_chunk} of {total_chunks} "
                             f"run chunks for user {user_id}.")
                for doc in docs:
                    doc_dict = doc.to_dict()
                    test_comp_scores = doc_dict.get('scores', {}).get('raw', {}).get('composite', {}).get('test', {})
                    doc_dict['num_attempted'] = test_comp_scores.get('numAttempted', None)
                    doc_dict['num_correct'] = test_comp_scores.get('numCorrect', None)
                    doc_dict['test_comp_theta_estimate'] = test_comp_scores.get('thetaEstimate', None)
                    doc_dict['test_comp_theta_se'] = test_comp_scores.get('thetaSE', None)

                    doc_dict.update({
                        'run_id': doc.id,
                        'user_id': user_id
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
        total_docs = self.db.collection('users').document(user_id).collection('runs').document(run_id).collection(
            'trials').get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                if os.environ.get('guest_mode', None):
                    if last_doc:
                        docs = (self.db.collection('guests')
                                .document(user_id)
                                .collection('runs')
                                .document(run_id)
                                .collection('trials')
                                .limit(chunk_size)
                                .start_after(last_doc)
                                .get())
                    else:
                        docs = (self.db.collection('guests')
                                .document(user_id)
                                .collection('runs')
                                .document(run_id)
                                .collection('trials')
                                .limit(chunk_size)
                                .get())
                else:
                    if last_doc:
                        docs = (self.db.collection('users')
                                .document(user_id)
                                .collection('runs')
                                .document(run_id)
                                .collection('trials')
                                .limit(chunk_size)
                                .start_after(last_doc)
                                .get())
                    else:
                        docs = (self.db.collection('users')
                                .document(user_id)
                                .collection('runs')
                                .document(run_id)
                                .collection('trials')
                                .limit(chunk_size)
                                .get())
                if not docs:
                    break
                current_chunk += 1
                logging.info(
                    f"Setting trials... processing chunk {current_chunk} of {total_chunks} "
                    f"trial chunks of run {run_id} for user {user_id}.")
                for doc in docs:
                    doc_dict = doc.to_dict()
                    # Add identifiers to the dictionary
                    doc_dict.update({
                        'trial_id': doc.id,
                        'user_id': user_id,
                        'run_id': run_id,
                        'task_id': task_id,
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
                    yield doc_dict
                last_doc = docs[-1]
            except Exception as e:
                logging.error(f"Error in get_trails: {e}")
                break

    def get_tasks(self, chunk_size=100):
        last_doc = None
        total_docs = self.db.collection('tasks').get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                if last_doc:
                    docs = self.db.collection('tasks').limit(chunk_size).start_after(last_doc).get()
                else:
                    docs = self.db.collection('tasks').limit(chunk_size).get()
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
        total_docs = self.db.collection('tasks').document(task_id).collection('variants').get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                if last_doc:
                    docs = (self.db.collection('tasks').document(task_id).collection('variants')
                            .limit(chunk_size).start_after(last_doc).get())
                else:
                    docs = (self.db.collection('tasks').document(task_id).collection('variants')
                            .limit(chunk_size).get())
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

    def get_assignments(self, lab_id: str, chunk_size=100):
        last_doc = None
        total_docs = self.db.collection('administrations').where('minimalOrgs.districts', 'array_contains',
                                                                 lab_id).get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                if last_doc:
                    docs = (self.db.collection('administrations')
                            .where('minimalOrgs.districts', 'array_contains', lab_id)
                            .limit(100)
                            .start_after(last_doc)
                            .get())
                else:
                    docs = (self.db.collection('administrations')
                            .where('minimalOrgs.districts', 'array_contains', lab_id)
                            .limit(100)
                            .get())
                if not docs:
                    break
                current_chunk += 1
                logging.info(f"Setting assignments... processing chunk {current_chunk} of {total_chunks} "
                             f"assignment chunks.")
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
                logging.error(f"Error in get_assignments: {e}")
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

    def get_trials_levante(self, user_id: str, run_id: str, task_id: str):
        result = []
        try:
            if os.environ.get('guest_mode', None):
                docs = (self.db.collection('guests').document(user_id).collection('runs').document(run_id).collection(
                    'trials')
                        .where('serverTimestamp', '>=', self.start_date)
                        .where('serverTimestamp', '<=', self.end_date)
                        .get())
            else:
                docs = (self.db.collection('users').document(user_id).collection('runs').document(run_id).collection(
                    'trials')
                        .where('serverTimestamp', '>=', self.start_date)
                        .where('serverTimestamp', '<=', self.end_date)
                        .get())

            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['trial_id'] = doc.id
                doc_dict['user_id'] = user_id
                doc_dict['run_id'] = run_id
                doc_dict['task_id'] = task_id

                doc_dict['trial_index'] = doc_dict.get('trialIndex', doc_dict.get('trial_index', None))
                doc_dict['is_correct'] = doc_dict.get('correct', None)
                doc_dict['is_practice'] = doc_dict.get('isPracticeTrial', None)
                doc_dict['corpus_trial_type'] = doc_dict.get('corpusTrialType', None)
                doc_dict['assessment_stage'] = doc_dict.get('assessment_stage', None)
                doc_dict['response_type'] = doc_dict.get('responseType', doc_dict.get('response_type', None))
                doc_dict['response_source'] = doc_dict.get('responseSource', doc_dict.get('response_source', None))

                doc_dict['time_elapsed'] = doc_dict.get('time_elapsed', None)
                doc_dict['server_timestamp'] = doc_dict.get('serverTimestamp', None)

                doc_dict['item'] = stringify_variables(doc_dict['item']) if doc_dict.get('item') is not None else None
                doc_dict['distract_options'] = stringify_variables(doc_dict['distractors']) if doc_dict.get(
                    'distractors') is not None else None
                doc_dict['expected_answer'] = stringify_variables(
                    doc_dict.get('answer', doc_dict.get('sequence', None)))
                doc_dict['response'] = stringify_variables(doc_dict.get('response', None))
                doc_dict['rt'] = stringify_variables(doc_dict['rt']) if doc_dict.get('rt') is not None else None

                doc_dict['theta_estimate'] = doc_dict.get('thetaEstimate', None)
                doc_dict['theta_estimate2'] = doc_dict.get('thetaEstimate2', None)
                doc_dict['theta_SE'] = doc_dict.get('thetaSE', None)
                doc_dict['theta_SE2'] = doc_dict.get('thetaSE2', None)

                result.append(doc_dict)
                # if isinstance(response_dict, dict) :
                #     doc_dict['item'] = stringify_variables(doc_dict['sequence']) if doc_dict.get('sequence') is not None else None
                #     rt_dict = doc_dict.get('rt', None)
                #     expected_answer_dict = doc_dict.get('sequence', None)
                #     for key, value in response_dict.items():
                #         sub_trial_dict = {'sub_trial_id': int(key), **doc_dict}
                #         sub_trial_dict['response'] = value
                #         sub_trial_dict['expected_answer'] = expected_answer_dict[key] if expected_answer_dict else None
                #         sub_trial_dict['rt'] = rt_dict[key] if rt_dict else None
                #         if sub_trial_dict['expected_answer'] is not None:
                #             sub_trial_dict['is_correct'] = True if sub_trial_dict['response'] == sub_trial_dict['expected_answer'] else False
                #         result.append(sub_trial_dict)
                # else:
                #     doc_dict['response'] = stringify_variables(doc_dict['response']) if doc_dict.get('response') is not None else None
                #     doc_dict['rt'] = stringify_variables(doc_dict['rt']) if doc_dict.get('rt') is not None else None
                #     result.append(doc_dict)

        except Exception as e:
            print(f"Error in get_trails: {e}")
        return result
