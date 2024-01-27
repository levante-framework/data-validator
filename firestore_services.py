import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os
import settings


class FirestoreServices:
    default_app = None
    db = None
    if 'local' in settings.DB_SITE:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = settings.SA_KEY_LOCATION_ADMIN

    def __init__(self, app_name):
        try:
            if app_name == 'assessment_site':
                cred = credentials.Certificate(os.environ['assessment_cred'])
            else:
                cred = credentials.ApplicationDefault()

            self.default_app = firebase_admin.initialize_app(credential=cred, name=app_name)
            self.db = firestore.client(self.default_app)
        except Exception as e:
            print(f"Error in {app_name} FirestoreService init: {e}")

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
            docs = self.db.collection('users').where('districts.current', 'array_contains', lab_id).get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['user_id'] = doc.id
                doc_dict['assessment_pid'] = doc_dict.pop('assessmentPid', None)
                doc_dict['assessment_uid'] = doc_dict.pop('assessmentUid', None)
                doc_dict['user_type'] = doc_dict.pop('userType', None)

                name = doc_dict.get('name', {})
                doc_dict['full_name'] = f"{name.get('first', '')} {name.get('middle', '')}. {name.get('last', '')}"

                student_data = doc_dict.get('studentData', {})
                doc_dict['dob'] = student_data.get('dob', None)
                doc_dict['gender'] = student_data.get('gender', None)
                doc_dict['grade'] = student_data.get('grade', None)
                doc_dict['hispanic_ethnicity'] = student_data.get('hispanic_ethnicity', None)
                doc_dict['state_id'] = student_data.get('state_id', None)
                doc_dict['races'] = ', '.join(student_data.get('race', []))

                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_users: {e}")
        return result

    def get_runs(self, user_id: str):
        result = []
        try:
            docs = self.db.collection('users').document(user_id).collection('runs').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['run_id'] = doc.id
                doc_dict['user_id'] = user_id
                doc_dict['task_id'] = doc_dict.pop('taskId', None)
                doc_dict['variant_id'] = doc_dict.pop('variantId', None)
                doc_dict['assignment_id'] = doc_dict.pop('assignmentId', None)
                doc_dict['is_completed'] = doc_dict.pop('completed', None)
                doc_dict['time_finished'] = doc_dict.pop('timeFinished', None)
                doc_dict['time_started'] = doc_dict.pop('timeStarted', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_runs: {e}")
        return result

    def get_trials(self, user_id: str, run_id: str, task_id: str):
        result = []
        try:
            docs = self.db.collection('users').document(user_id).collection('runs').document(run_id).collection(
                'trials').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['trial_id'] = doc.id
                doc_dict['user_id'] = user_id
                doc_dict['run_id'] = run_id
                doc_dict['task_id'] = task_id
                doc_dict['subtask_id'] = doc_dict.pop('subtask', None)
                doc_dict['is_practice'] = True if 'practice' in doc_dict.get('assessment_stage', '') else False
                doc_dict['corpus_id'] = doc_dict.pop('corpusId', None)
                doc_dict['is_correct'] = True if doc_dict.get('correct', '') == 1 else False
                doc_dict['response'] = doc_dict.get('responseValue', None) or doc_dict.get('response',
                                                                                           None) or doc_dict.get(
                    'keyboard_response', None)
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
                params = doc_dict.get('params', {})
                doc_dict['consent'] = params.get('consent', None)
                doc_dict['recruitment'] = params.get('recruitment', '')
                doc_dict['skip_instructions'] = params.get('skipInstructions', None)
                doc_dict['story'] = params.get('story', None)
                doc_dict['user_mode'] = params.get('user_mode', None)
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
