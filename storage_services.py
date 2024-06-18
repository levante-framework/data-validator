from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime
import json
import os

import settings

# Create a client
if 'local' in os.environ['ENV']:
    cred = service_account.Credentials.from_service_account_file(filename=os.getenv('LOCAL_ADMIN_SERVICE_ACCOUNT'))
    storage_client = storage.Client(credentials=cred)
else:
    storage_client = storage.Client()


def upload_blob_from_memory(bucket_name, data, destination_blob_name, content_type):
    """
        Uploads a file from memory to Google Cloud Storage.

        Args:
        - bucket_name (str): Name of the GCS bucket.
        - data (bytes or str): Data to upload.
        - destination_blob_name (str): Desired name for the file in the bucket.
        - content_type (str): Content type of the file (e.g., 'application/json', 'text/csv').
        """
    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a blob object
    blob = bucket.blob(destination_blob_name)

    # Upload the file
    blob.upload_from_string(data, content_type=content_type)


class StorageServices:
    storage_prefix = None

    def __init__(self, lab_id: str, is_from_firestore: bool):
        self.lab_id = lab_id
        self.source = "firestore" if is_from_firestore else "redivis"
        self.timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        self.storage_prefix = f"lab_{self.lab_id}_{self.source}_{self.timestamp}/"
        self.upload_to_GCP_log = []

    def process(self, valid_data: dict, invalid_data: dict):
        for key, value in valid_data.items():
            if value:
                self.save_to_storage(table_name=key, data=value)

        if invalid_data:
            self.save_to_storage(table_name="validation_results", data=invalid_data)
        # self.save_to_storage(table_name="data_upload_logs", data=self.upload_to_GCP_log)

    def save_to_storage(self, table_name: str, data):
        data_json = json.dumps(data, cls=CustomJSONEncoder)
        destination_blob_name = f"lab_{self.lab_id}_{self.source}_{self.timestamp}/{table_name}.json"
        try:
            upload_blob_from_memory(bucket_name=settings.config['CORE_DATA_BUCKET_NAME'], data=data_json,
                                    destination_blob_name=destination_blob_name,
                                    content_type='application/json')
            self.upload_to_GCP_log.append(f"Data uploaded to {destination_blob_name}.")
        except Exception as e:
            self.upload_to_GCP_log.append(
                f"Failed to save {self.source} data to cloud, {self.lab_id}, {table_name}, {e}")

    def list_blobs_with_prefix(self, delimiter=None):
        """Lists all the blobs in the bucket that begin with the prefix."""
        blobs = storage_client.list_blobs(settings.config['CORE_DATA_BUCKET_NAME'], prefix=self.storage_prefix, delimiter=delimiter)

        return [blob.name for blob in blobs if 'log' not in blob.name]


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)
