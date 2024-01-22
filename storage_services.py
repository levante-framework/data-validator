from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime
import json
import settings
from entity_controller import EntityController


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


class StorageServices:
    def __init__(self):
        pass

    def firestore_to_storage(self, lab_id, assessment_cred, source):
        ec = EntityController(lab_id)
        ec.set_values_from_firestore(assessment_cred)
        for key, value in self.get_valid_data(ec).items():
            self.save_to_storage(lab_id=lab_id, table_name=key, data=value, source=source)
        self.save_to_storage(lab_id=lab_id, table_name="invalid_log", data=self.get_invalid_data(ec), source=source)

    def redivis_to_storage(self, lab_id, table_name):
        pass

    def get_valid_data(self, ec: EntityController):
        valid_dict = {
            'districts': [obj.model_dump() for obj in ec.valid_districts],
            # 'schools': [obj.model_dump() for obj in ec.valid_schools],
            # 'classes': [obj.model_dump() for obj in ec.valid_classes],
            'users': [obj.model_dump() for obj in ec.valid_users],
            'runs': [obj.model_dump() for obj in ec.valid_runs],
            'trials': [obj.model_dump() for obj in ec.valid_trials],
            'assignments': [obj.model_dump() for obj in ec.valid_assignments],
            'tasks': [obj.model_dump() for obj in ec.valid_tasks],
            # 'variants': [obj.model_dump() for obj in ec.valid_variants],
            # 'variants_params': [obj.model_dump() for obj in ec.valid_variants_params],
            # 'user_classes': [obj.model_dump() for obj in ec.valid_user_class],
            # 'user_assignments': [obj.model_dump() for obj in ec.valid_user_assignment],
            # 'assignment_tasks': [obj.model_dump() for obj in ec.valid_assignment_task]
        }
        return valid_dict

    def get_invalid_data(self, ec: EntityController):
        invalid_dict = {
            'districts': [obj for obj in ec.invalid_districts],
            'schools': [obj for obj in ec.invalid_schools],
            'classes': [obj for obj in ec.invalid_classes],
            'users': [obj for obj in ec.invalid_users],
            'runs': [obj for obj in ec.invalid_runs],
            'trials': [obj for obj in ec.invalid_trials],
            'assignments': [obj for obj in ec.invalid_assignments],
            'tasks': [obj for obj in ec.invalid_tasks],
            'variants': [obj for obj in ec.invalid_variants],
            'variants_params': [obj for obj in ec.invalid_variants_params],
            'user_classes': [obj for obj in ec.invalid_user_class],
            'user_assignments': [obj for obj in ec.invalid_user_assignment],
            'assignment_tasks': [obj for obj in ec.invalid_assignment_task]
        }
        return invalid_dict

    def save_to_storage(self, lab_id: str, table_name: str, data, source):

        data_json = json.dumps(data, cls=CustomJSONEncoder)
        if settings.SAVE_TO_STORAGE:
            try:
                self.upload_blob_from_memory(bucket_name=settings.BUCKET_NAME, data=data_json,
                                             destination_blob_name=f"lab_{lab_id}_{source}_{table_name}_{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.json",
                                             content_type='application/json')
            except Exception as e:
                print(f"Failed to save roar data to cloud, {lab_id}, {table_name}, {e}")
        else:
            with open(f'lab_{lab_id}_{table_name}.json', 'w') as file:
                file.write(data_json)

    def upload_blob_from_memory(self, bucket_name, data, destination_blob_name, content_type):
        """
            Uploads a file from memory to Google Cloud Storage.

            Args:
            - bucket_name (str): Name of the GCS bucket.
            - data (bytes or str): Data to upload.
            - destination_blob_name (str): Desired name for the file in the bucket.
            - content_type (str): Content type of the file (e.g., 'application/json', 'text/csv').
            """
        # Create a client
        if "local" in settings.DB_SITE:
            cred = service_account.Credentials.from_service_account_file(filename=settings.SA_KEY_LOCATION_ADMIN)
            storage_client = storage.Client(credentials=cred)
        else:
            storage_client = storage.Client()

        # Get the bucket
        bucket = storage_client.bucket(bucket_name)

        # Create a blob object
        blob = bucket.blob(destination_blob_name)

        # Upload the file
        try:
            blob.upload_from_string(data, content_type=content_type)
            print(f"Data uploaded to {destination_blob_name}.")
        except Exception as e:
            print(f"Failed to upload to {destination_blob_name}, {e}")
