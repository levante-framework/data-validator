from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime
import json
import settings
from entity_controller import EntityController


def upload_blob_from_memory(bucket_name, data, destination_blob_name, content_type):
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
    blob.upload_from_string(data, content_type=content_type)


class StorageServices:
    storage_prefix = None

    def __init__(self, lab_id: str, source: str):
        self.lab_id = lab_id
        self.source = source
        self.ec = EntityController(lab_id)
        self.timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        self.storage_prefix = f"lab_{self.lab_id}_{source}_{self.timestamp}/"

    def process(self):
        if self.source == 'firestore':
            self.ec.set_values_from_firestore()
        else:
            pass

        for key, value in self.get_valid_data().items():
            self.save_to_storage(table_name=key, data=value)
        self.save_to_storage(table_name="validation_log", data=self.get_invalid_data())

    def get_valid_data(self):
        valid_dict = {
            'districts': [obj.model_dump() for obj in self.ec.valid_districts],
            # 'schools': [obj.model_dump() for obj in self.ec.valid_schools],
            'classes': [obj.model_dump() for obj in self.ec.valid_classes],
            'users': [obj.model_dump() for obj in self.ec.valid_users],
            'runs': [obj.model_dump() for obj in self.ec.valid_runs],
            'trials': [obj.model_dump() for obj in self.ec.valid_trials],
            'assignments': [obj.model_dump() for obj in self.ec.valid_assignments],
            'tasks': [obj.model_dump() for obj in self.ec.valid_tasks],
            # 'variants': [obj.model_dump() for obj in self.ec.valid_variants],
            # 'variants_params': [obj.model_dump() for obj in self.ec.valid_variants_params],
            'user_classes': [obj.model_dump() for obj in self.ec.valid_user_class],
            # 'user_assignments': [obj.model_dump() for obj in self.ec.valid_user_assignment],
            # 'assignment_tasks': [obj.model_dump() for obj in self.ec.valid_assignment_task]
        }
        return valid_dict

    def get_invalid_data(self):
        invalid_dict = {
            'districts': [obj for obj in self.ec.invalid_districts],
            'schools': [obj for obj in self.ec.invalid_schools],
            'classes': [obj for obj in self.ec.invalid_classes],
            'users': [obj for obj in self.ec.invalid_users],
            'runs': [obj for obj in self.ec.invalid_runs],
            'trials': [obj for obj in self.ec.invalid_trials],
            'assignments': [obj for obj in self.ec.invalid_assignments],
            'tasks': [obj for obj in self.ec.invalid_tasks],
            'variants': [obj for obj in self.ec.invalid_variants],
            'variants_params': [obj for obj in self.ec.invalid_variants_params],
            'user_classes': [obj for obj in self.ec.invalid_user_class],
            'user_assignments': [obj for obj in self.ec.invalid_user_assignment],
            'assignment_tasks': [obj for obj in self.ec.invalid_assignment_task]
        }
        return invalid_dict

    def save_to_storage(self, table_name: str, data):
        data_json = json.dumps(data, cls=CustomJSONEncoder)
        destination_blob_name = f"lab_{self.lab_id}_{self.source}_{self.timestamp}/{table_name}.json"
        try:
            upload_blob_from_memory(bucket_name=settings.BUCKET_NAME, data=data_json,
                                    destination_blob_name=destination_blob_name,
                                    content_type='application/json')
            print(f"Data uploaded to {destination_blob_name}.")
        except Exception as e:
            print(f"Failed to save {self.source} data to cloud, {self.lab_id}, {table_name}, {e}")

    def list_blobs_with_prefix(self, delimiter=None):
        """Lists all the blobs in the bucket that begin with the prefix."""
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(settings.BUCKET_NAME, prefix=self.storage_prefix, delimiter=delimiter)

        return [blob.name for blob in blobs if 'log' not in blob.name]


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)
