import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os
import settings
import json


class FirestoreServices:
    default_app = None
    db = None
    if 'local' in settings.DB_SITE:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = settings.SA_KEY_LOCATION_ADMIN

    def __init__(self, app_name):
        try:
            if app_name == 'assessment_site':
                cred = credentials.Certificate(json.loads(os.environ['assessment_cred']))
            else:
                cred = credentials.ApplicationDefault()

            self.default_app = firebase_admin.initialize_app(credential=cred, name=app_name)
            self.db = firestore.client(self.default_app)
        except Exception as e:
            print(f"Error in {app_name} FirestoreService init: {e}")

    def get_groups(self, lab_id: str):
        result = []
        try:
            doc = self.db.collection('groups').document(lab_id).get()
            doc_dict = doc.to_dict()  # Convert the document to a dictionary
            doc_dict['group_id'] = doc.id
            doc_dict['abbreviation'] = doc_dict.pop('abbreviation', None)
            result.append(doc_dict)

        except Exception as e:
            print(f"Error in get_groups: {e}")
        return result

    def get_districts(self, lab_id: str):
        result = []
        try:
            doc = self.db.collection('districts').document(lab_id).get()
            doc_dict = doc.to_dict()  # Convert the document to a dictionary
            doc_dict['district_id'] = doc.id
            doc_dict['portal_url'] = doc_dict.pop('portalUrl', None)

            district_contact = doc_dict.get('districtContact', {})
            district_contact_name = district_contact.get('name', {})
            doc_dict[
                'district_contact_name'] = f"{district_contact_name.get('first', '')}, {district_contact_name.get('last', '')}"
            doc_dict['district_contact_email'] = district_contact.get('email', None)
            doc_dict['district_contact_title'] = district_contact.get('title', None)
            doc_dict['last_sync'] = doc_dict.pop('lastSync', None)
            doc_dict['launch_date'] = doc_dict.pop('launchDate', None)
            result.append(doc_dict)

        except Exception as e:
            print(f"Error in get_districts: {e}")
        return result

    def get_schools(self, lab_id: str):
        result = []
        try:
            docs = self.db.collection('schools').where('districtId', '==', lab_id).get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['school_id'] = doc.id
                doc_dict['district_id'] = doc_dict.pop('districtId', None)
                doc_dict['state_id'] = doc_dict.pop('stateId', None)
                doc_dict['school_number'] = doc_dict.pop('schoolNumber', None)
                doc_dict['high_grade'] = doc_dict.pop('highGrade', None)
                doc_dict['low_grade'] = doc_dict.pop('lowGrade', None)

                location = doc_dict.get('location', {})
                doc_dict['address'] = location.get('address', None)
                doc_dict['city'] = location.get('city', None)
                doc_dict['state'] = location.get('state', None)
                doc_dict['zip'] = location.get('zip', None)

                principal = doc_dict.get('principal', {})
                doc_dict['principal_name'] = principal.get('name', None)
                doc_dict['principal_email'] = principal.get('email', None)

                doc_dict['last_modified'] = doc_dict.pop('lastModified', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_schools: {e}")
        return result

    def get_classes(self, lab_id: str):
        result = []
        try:
            docs = self.db.collection('classes').where('districtId', '==', lab_id).get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['class_id'] = doc.id
                doc_dict['school_id'] = doc_dict.pop('schoolId', None)
                doc_dict['district_id'] = doc_dict.pop('districtId', None)
                doc_dict['section_number'] = doc_dict.pop('sectionNumber', None)
                doc_dict['last_modified'] = doc_dict.pop('lastModified', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_classes: {e}")
        return result

    def get_users(self, lab_id: str):
        result = []
        try:
            if settings.MODE == 'guest':
                docs = self.db.collection('guests').get()
            else:
                docs = self.db.collection('users').where('groups.current', 'array_contains', lab_id).get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['user_id'] = doc.id
                doc_dict['user_type'] = doc_dict.get('userType', None)
                doc_dict['assessment_uid'] = doc_dict.get('assessmentUid', None)
                doc_dict['parent_id'] = doc_dict.get('parentId', None)
                doc_dict['teacher_id'] = doc_dict.get('teacherId', None)
                doc_dict['birth_year'] = doc_dict.get('birthYear', None)
                doc_dict['birth_month'] = doc_dict.get('birthMonth', None)
                doc_dict['email_verified'] = doc_dict.get('emailVerified', None)

                doc_dict['created_at'] = doc_dict.get('createdAt', None)
                doc_dict['last_updated'] = doc_dict.get('lastUpdated', None)

                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_users: {e}")
        return result

    def get_runs(self, user_id: str):
        result = []
        try:
            if settings.MODE == 'guest':
                docs = self.db.collection('guests').document(user_id).collection('runs').get()
            else:
                docs = self.db.collection('users').document(user_id).collection('runs').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['run_id'] = doc.id
                doc_dict['user_id'] = user_id
                doc_dict['task_id'] = doc_dict.get('taskId', None)
                doc_dict['variant_id'] = doc_dict.get('variantId', None)
                doc_dict['assignment_id'] = doc_dict.get('assignmentId', None)
                doc_dict['assigning_orgs'] = doc_dict.get('assigningOrgs', None)
                doc_dict['is_completed'] = doc_dict.get('completed', None)
                doc_dict['time_finished'] = doc_dict.get('timeFinished', None)
                doc_dict['time_started'] = doc_dict.get('timeStarted', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_runs: {e}")
        return result

    def get_trials(self, user_id: str, run_id: str, task_id: str):
        result = []
        try:
            if settings.MODE == 'guest':
                docs = self.db.collection('guests').document(user_id).collection('runs').document(
                    run_id).collection('trials').get()
            else:
                docs = self.db.collection('users').document(user_id).collection('runs').document(
                    run_id).collection('trials').get()

            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['trial_id'] = doc.id
                doc_dict['user_id'] = user_id
                doc_dict['run_id'] = run_id
                doc_dict['task_id'] = task_id

                doc_dict['distract_options'] = str(doc_dict.get('distractors', None))
                doc_dict['expected_answer'] = doc_dict.get('answer', None)
                doc_dict['response_type'] = doc_dict.get('response_type', None)
                doc_dict['response_source'] = doc_dict.get('response_source', None)
                doc_dict['is_correct'] = doc_dict.get('correct', None)

                doc_dict['is_practice'] = doc_dict.get('isPracticeTrial', None)
                doc_dict['server_timestamp'] = doc_dict.pop('serverTimestamp', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_trails: {e}")
        return result

    def get_tasks(self):
        result = []
        try:
            docs = self.db.collection('tasks').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['task_id'] = doc.id  # Add the document ID under the key 'id'
                doc_dict['last_updated'] = doc_dict.pop('lastUpdated', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_tasks: {e}")
        return result

    def get_variants(self, task_id: str):
        result = []
        try:
            docs = self.db.collection('tasks').document(task_id).collection('variants').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['variant_id'] = doc.id  # Add the document ID under the key 'id'
                doc_dict['task_id'] = task_id
                # params = doc_dict.get('params', {})
                # doc_dict['consent'] = params.get('consent', None)
                # doc_dict['recruitment'] = params.get('recruitment', '')
                # doc_dict['skip_instructions'] = params.get('skipInstructions', None)
                # doc_dict['story'] = params.get('story', None)
                # doc_dict['user_mode'] = params.get('user_mode', None)
                doc_dict['last_updated'] = doc_dict.get('lastUpdated', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_variants: {e}")
        return result

    def get_assignments(self):
        result = []
        try:
            docs = self.db.collection('administrations').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['assignment_id'] = doc.id  # Add the document ID under the key 'id'
                doc_dict['is_sequential'] = doc_dict.pop('sequential', None)
                doc_dict['created_by'] = doc_dict.pop('createdBy', None)
                doc_dict['date_created'] = doc_dict.pop('dateCreated', None)
                doc_dict['date_closed'] = doc_dict.pop('dateClosed', None)
                doc_dict['date_opened'] = doc_dict.pop('dateOpened', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_assignments: {e}")
        return result

