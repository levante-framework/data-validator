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