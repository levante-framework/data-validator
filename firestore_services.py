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
                cred = credentials.Certificate(settings.DB_KEY_LOCATION)
            else:
                cred = credentials.ApplicationDefault()
            self.default_app = firebase_admin.initialize_app(cred, {"projectId": self.project_id})
            self.db = firestore.client(self.default_app)
        except Exception as e:
            print(f"Error in FirestoreService init: {e}")

    def get_schools(self):
        result = []
        try:
            docs = self.db.collection('schools').get()
            result = [doc.to_dict() for doc in docs]
        except Exception as e:
            print(f"Error in get_schools: {e}")
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
