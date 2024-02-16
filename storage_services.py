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

    def __init__(self, lab_id: str, is_from_firestore: bool, dataset_version):
        self.lab_id = lab_id
        self.source = "firestore" if is_from_firestore else "redivis"
        self.ec = EntityController(lab_id=lab_id, source=self.source)
        if self.source == 'firestore':
            self.ec.set_values_from_firestore()
        else:
            self.ec.set_values_from_redivis(dataset_version=dataset_version)

        self.timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        self.storage_prefix = f"lab_{self.lab_id}_{self.source}_{self.timestamp}/"
        self.data_upload_log = []

    def process(self):
        for key, value in self.get_valid_data().items():
            self.save_to_storage(table_name=key, data=value)

        self.save_to_storage(table_name="validation_results", data=self.get_invalid_data())
        self.save_to_storage(table_name="data_upload_logs", data=self.data_upload_log)

    def get_valid_data(self):
        valid_dict = {
            'districts': [obj.model_dump() for obj in self.ec.valid_districts],
            'schools': [obj.model_dump() for obj in self.ec.valid_schools],
            'classes': [obj.model_dump() for obj in self.ec.valid_classes],
            'users': [obj.model_dump() for obj in self.ec.valid_users],
            'runs': [obj.model_dump() for obj in self.ec.valid_runs],
            'trials': [obj.model_dump() for obj in self.ec.valid_trials],
            'assignments': [obj.model_dump() for obj in self.ec.valid_assignments],
            'tasks': [obj.model_dump() for obj in self.ec.valid_tasks],
            'variants': [obj.model_dump() for obj in self.ec.valid_variants]
        }
        if self.source == "firestore":
            valid_dict['variants_params'] = [obj.model_dump() for obj in self.ec.valid_variants_params]
            valid_dict['user_classes'] = [obj.model_dump() for obj in self.ec.valid_user_class]
            valid_dict['user_assignments'] = [obj.model_dump() for obj in self.ec.valid_user_assignment]
            valid_dict['assignment_tasks'] = [obj.model_dump() for obj in self.ec.valid_assignment_task]
        return valid_dict

    def get_invalid_data(self):
        invalid_list = ([{**obj, "table_name": "districts"} for obj in self.ec.invalid_districts]
                        + [{**obj, "table_name": "schools"} for obj in self.ec.invalid_schools]
                        + [{**obj, "table_name": "classes"} for obj in self.ec.invalid_classes]
                        + [{**obj, "table_name": "users"} for obj in self.ec.invalid_users]
                        + [{**obj, "table_name": "runs"} for obj in self.ec.invalid_runs]
                        + [{**obj, "table_name": "trials"} for obj in self.ec.invalid_trials]
                        + [{**obj, "table_name": "assignments"} for obj in self.ec.invalid_assignments]
                        + [{**obj, "table_name": "tasks"} for obj in self.ec.invalid_tasks]
                        + [{**obj, "table_name": "variants"} for obj in self.ec.invalid_variants])

        if self.source == "firestore":
            invalid_list = (invalid_list
                            + [{**obj, "table_name": "variants_params"} for obj in self.ec.invalid_variants_params]
                            + [{**obj, "table_name": "user_class"} for obj in self.ec.invalid_user_class]
                            + [{**obj, "table_name": "user_assignment"} for obj in self.ec.invalid_user_assignment]
                            + [{**obj, "table_name": "assignment_task"} for obj in self.ec.invalid_assignment_task])

        for invalid_item in invalid_list:
            if 'loc' in invalid_item:
                invalid_item['loc'] = invalid_item['loc'][0]
            if 'input' in invalid_item:
                invalid_item['input'] = str(invalid_item['input'])
            if 'url' in invalid_item:
                invalid_item.pop('url')

        return invalid_list

    def save_to_storage(self, table_name: str, data):
        data_json = json.dumps(data, cls=CustomJSONEncoder)
        destination_blob_name = f"lab_{self.lab_id}_{self.source}_{self.timestamp}/{table_name}.json"
        try:
            upload_blob_from_memory(bucket_name=settings.BUCKET_NAME, data=data_json,
                                    destination_blob_name=destination_blob_name,
                                    content_type='application/json')
            print(f"Data uploaded to {destination_blob_name}.")
            self.data_upload_log.append(f"Data uploaded to {destination_blob_name}.")
        except Exception as e:
            print(f"Failed to save {self.source} data to cloud, {self.lab_id}, {table_name}, {e}")
            self.data_upload_log.append(f"Failed to save {self.source} data to cloud, {self.lab_id}, {table_name}, {e}")

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
