import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import os
import settings


class FirestoreServices:
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', None)
    default_app = None
    db = None

    def __init__(self):
        try:
            if "local" in settings.DB_SITE:
                cred = credentials.Certificate(settings.DB_KEY_LOCATION_ADMIN)
            else:
                cred = credentials.ApplicationDefault()
            self.default_app = firebase_admin.initialize_app(cred, {"projectId": self.project_id})
            self.db = firestore.client(self.default_app)
        except Exception as e:
            print(f"Error in FirestoreService init: {e}")

    def get_districts(self):
        result = []
        try:
            docs = self.db.collection('districts').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['portal_url'] = doc_dict.pop('portalUrl', None)

                district_contact = doc_dict.get('districtContact', {})
                district_contact_name = district_contact.get('name', {})
                doc_dict[
                    'district_contact_name'] = f"{district_contact_name.get('first', None)}, {district_contact_name.get('last', None)}"
                doc_dict['district_contact_email'] = district_contact.get('email', None)
                doc_dict['district_contact_title'] = district_contact.get('title', None)
                doc_dict['last_sync'] = doc_dict.pop('lastSync', None)
                doc_dict['launch_date'] = doc_dict.pop('launchDate', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_districts: {e}")
        return result

    def get_classes(self):
        result = []
        try:
            docs = self.db.collection('classes').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['school_id'] = doc_dict.pop('schoolId', None)
                doc_dict['district_id'] = doc_dict.pop('districtId', None)
                doc_dict['section_number'] = doc_dict.pop('sectionNumber', None)
                doc_dict['last_modified'] = doc_dict.pop('lastModified', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_classes: {e}")
        return result

    def get_schools(self):
        result = []
        try:
            docs = self.db.collection('schools').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
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

    def get_users(self):
        result = []
        try:
            docs = self.db.collection('users').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['school_id'] = doc_dict.pop('schoolId', None)
                doc_dict['district_id'] = doc_dict.pop('districtId', None)
                doc_dict['section_number'] = doc_dict.pop('sectionNumber', None)
                doc_dict['last_modified'] = doc_dict.pop('lastModified', None)
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_classes: {e}")
        return result

    def get_tasks(self):
        result = []
        try:
            docs = self.db.collection('tasks').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['id'] = doc.id  # Add the document ID under the key 'id'
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_tasks: {e}")
        return result

    def get_variants(self, task_id):
        result = []
        try:
            docs = self.db.collection('tasks').document(task_id).collection('variants').get()
            for doc in docs:
                doc_dict = doc.to_dict()  # Convert the document to a dictionary
                doc_dict['id'] = doc.id  # Add the document ID under the key 'id'
                result.append(doc_dict)
        except Exception as e:
            print(f"Error in get_tasks: {e}")
        return result
