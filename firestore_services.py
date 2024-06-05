import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os
import logging
from datetime import datetime
import json
from utils import process_doc_dict, handle_nan

logging.basicConfig(level=logging.INFO)


class FirestoreServices:
    default_app = None
    db = None

    def __init__(self, app_name):
        try:
            # Check if the app already exists
            self.default_app = firebase_admin.get_app(name=app_name)
        except ValueError:
            # If the app does not exist, initialize it based on the app_name
            if app_name == 'assessment_site':
                cred = credentials.Certificate(json.loads(os.environ['assessment_cred']))
            else:
                cred = credentials.ApplicationDefault()

            self.default_app = firebase_admin.initialize_app(credential=cred, name=app_name)

        self.db = firestore.client(self.default_app)

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
        total_docs = self.db.collection('users').where('districts.current', 'array_contains', lab_id).get()
        total_chunks = len(total_docs) // chunk_size + (len(total_docs) % chunk_size > 0)
        current_chunk = 0
        while True:
            try:
                if os.environ.get('guest_mode', None):
                    if last_doc:
                        docs = (self.db.collection('guests')
                                .limit(chunk_size)
                                .start_after(last_doc)
                                .get())
                    else:
                        docs = (self.db.collection('guests')
                                .limit(chunk_size)
                                .get())
                else:
                    if last_doc:
                        docs = (self.db.collection('users')
                                .where('districts.current', 'array_contains', lab_id)
                                .limit(chunk_size)
                                .start_after(last_doc)
                                .get())
                    else:
                        docs = (self.db.collection('users')
                                .where('districts.current', 'array_contains', lab_id)
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
        total_docs = self.db.collection('users').document(user_id).collection('runs').document(run_id).collection('trials').get()
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
        total_docs = self.db.collection('administrations').where('minimalOrgs.districts', 'array_contains', lab_id).get()
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
