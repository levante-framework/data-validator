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
        result = []
        try:
            doc = self.db.collection('groups').document(lab_id).get()
            doc_dict = doc.to_dict()
            doc_dict.update({
                'group_id': doc.id,
            })
            # Convert camelCase to snake_case and handle NaN values
            converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
            result.append(converted_doc_dict)
        except Exception as e:
            logging.error(f"Error in get_groups: {e}")
        return result

    def get_districts(self, lab_id: str):
        result = []
        try:
            doc = self.db.collection('districts').document(lab_id).get()
            doc_dict = doc.to_dict()
            doc_dict.update({
                'district_id': doc.id,
            })
            # Convert camelCase to snake_case and handle NaN values
            converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
            result.append(converted_doc_dict)
        except Exception as e:
            logging.error(f"Error in get_districts: {e}")
        return result

    def get_schools(self, lab_id: str):
        result = []
        try:
            docs = self.db.collection('schools').where('districtId', '==', lab_id).get()
            for doc in docs:
                doc_dict = doc.to_dict()
                doc_dict.update({
                    'school_id': doc.id,
                })
                # Convert camelCase to snake_case and handle NaN values
                converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                result.append(converted_doc_dict)
        except Exception as e:
            logging.error(f"Error in get_schools: {e}")
        return result

    def get_classes(self, lab_id: str):
        result = []
        try:
            docs = self.db.collection('classes').where('districtId', '==', lab_id).get()
            for doc in docs:
                doc_dict = doc.to_dict()
                doc_dict.update({
                    'class_id': doc.id,
                })
                # Convert camelCase to snake_case and handle NaN values
                converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                result.append(converted_doc_dict)
        except Exception as e:
            logging.error(f"Error in get_classes: {e}")
        return result

    def get_users(self, lab_id: str, start_date, end_date):
        result = []
        if start_date:
            start_date = datetime.strptime(start_date, "%m/%d/%Y").replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            start_date = datetime(2024, 1, 1)
        if end_date:
            end_date = datetime.strptime(end_date, '%m/%d/%Y').replace(hour=23, minute=59, second=59,
                                                                       microsecond=999999)
        else:
            end_date = datetime(2030, 1, 1)

        try:
            if os.environ.get('guest_mode', None):
                docs = (self.db.collection('guests')
                        .where('createdAt', '>=', start_date)
                        .where('createdAt', '<=', end_date)
                        .get())
            else:
                docs = (self.db.collection('users')
                        # .where('groups.current', 'array_contains', lab_id)
                        .where('districts.current', 'array_contains', lab_id)
                        .where('createdAt', '>=', start_date)
                        .where('createdAt', '<=', end_date)
                        .get())
            for doc in docs:
                doc_dict = doc.to_dict()
                doc_dict.update({
                    'user_id': doc.id,
                })
                # Convert camelCase to snake_case and handle NaN values
                converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                result.append(converted_doc_dict)
        except Exception as e:
            logging.error(f"Error in get_users: {e}")
        return result

    def get_runs(self, user_id: str):
        result = []
        try:
            if os.environ.get('guest_mode', None):
                docs = self.db.collection('guests').document(user_id).collection('runs').get()
            else:
                docs = self.db.collection('users').document(user_id).collection('runs').get()
            for doc in docs:
                doc_dict = doc.to_dict()
                doc_dict.update({
                    'run_id': doc.id,
                    'user_id': user_id
                })
                # Convert camelCase to snake_case and handle NaN values
                converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                result.append(converted_doc_dict)
        except Exception as e:
            logging.error(f"Error in get_runs: {e}")
        return result

    def get_trials(self, user_id: str, run_id: str, task_id: str):
        result = []
        try:
            if os.environ.get('guest_mode', None):
                docs = self.db.collection('guests').document(user_id).collection('runs').document(
                    run_id).collection('trials').get()
            else:
                docs = self.db.collection('users').document(user_id).collection('runs').document(
                    run_id).collection('trials').get()

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
                # Append the formatted dictionary to the result list
                result.append(doc_dict)
        except Exception as e:
            logging.error(f"Error in get_trails: {e}")
        return result

    def get_tasks(self):
        result = []
        try:
            docs = self.db.collection('tasks').get()
            for doc in docs:
                doc_dict = doc.to_dict()
                doc_dict.update({
                    'task_id': doc.id
                })
                # Convert camelCase to snake_case and handle NaN values
                converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                result.append(converted_doc_dict)
        except Exception as e:
            logging.error(f"Error in get_tasks: {e}")
        return result

    def get_variants(self, task_id: str):
        result = []
        try:
            docs = self.db.collection('tasks').document(task_id).collection('variants').get()
            for doc in docs:
                doc_dict = doc.to_dict()
                doc_dict.update({
                    'variant_id': doc.id,
                    'task_id': task_id
                })
                # Convert camelCase to snake_case and handle NaN values
                converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                result.append(converted_doc_dict)
        except Exception as e:
            logging.error(f"Error in get_variants: {e}")
        return result

    def get_assignments(self, lab_id: str):
        result = []
        try:
            docs = self.db.collection('administrations').where('minimalOrgs.districts', 'array_contains', lab_id).get()
            for doc in docs:
                doc_dict = doc.to_dict()
                doc_dict.update({
                    'assignment_id': doc.id,
                })
                # Convert camelCase to snake_case and handle NaN values
                converted_doc_dict = process_doc_dict(doc_dict=doc_dict)
                result.append(converted_doc_dict)
        except Exception as e:
            logging.error(f"Error in get_assignments: {e}")
        return result
