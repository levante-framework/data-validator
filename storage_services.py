from google.cloud import storage
from google.oauth2 import service_account
import datetime
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

    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        self.storage_prefix = f"{self.dataset_id}/"
        self.upload_to_GCP_log = []

    def process(self, valid_data: dict, invalid_data: list, validation_logs: list):
        for key, value in valid_data.items():
            if value:
                self.save_to_storage(table_name=key, data=value)

        if invalid_data:
            self.save_to_storage(table_name="validation_results", data=invalid_data)
        else:
            [dct.update({'date_time': datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d-%H-%M-%S UTC")}) for dct in validation_logs]
            self.save_to_storage(table_name="validation_results", data=validation_logs)

    def save_to_storage(self, table_name: str, data):
        data_json = json.dumps(data, cls=CustomJSONEncoder)
        destination_blob_name = f"{self.dataset_id}/{table_name}.json"
        try:
            upload_blob_from_memory(bucket_name=settings.config['CORE_DATA_BUCKET_NAME'], data=data_json,
                                    destination_blob_name=destination_blob_name,
                                    content_type='application/json')
            self.upload_to_GCP_log.append(f"Data uploaded to {destination_blob_name}.")
        except Exception as e:
            self.upload_to_GCP_log.append(
                f"Failed to save data to cloud, {self.dataset_id}, {table_name}, {e}")

    def list_blobs_with_prefix(self, delimiter=None):
        """Lists all the blobs in the bucket that begin with the prefix."""
        blobs = storage_client.list_blobs(settings.config['CORE_DATA_BUCKET_NAME'], prefix=self.storage_prefix, delimiter=delimiter)

        return [blob.name for blob in blobs]


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)
