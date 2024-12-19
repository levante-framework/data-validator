from google.cloud import storage
from google.oauth2 import service_account
import datetime
import json
import os
import logging

import settings

logging.basicConfig(level=logging.INFO)

# Create a client
if 'local' in os.environ['ENV']:
    cred = service_account.Credentials.from_service_account_file(filename=os.getenv('LOCAL_ADMIN_SERVICE_ACCOUNT'))
    storage_client = storage.Client(credentials=cred)
else:
    storage_client = storage.Client()

gcp_bucket = storage_client.bucket(settings.config['CORE_DATA_BUCKET_NAME'])


def upload_blob_from_memory(data, destination_blob_name, content_type):
    """
        Uploads a file from memory to Google Cloud Storage.

        Args:
        - bucket_name (str): Name of the GCS bucket.
        - data (bytes or str): Data to upload.
        - destination_blob_name (str): Desired name for the file in the bucket.
        - content_type (str): Content type of the file (e.g., 'application/json', 'text/csv').
        """
    # Create a blob object
    blob = gcp_bucket.blob(destination_blob_name)

    # Upload the file
    blob.upload_from_string(data, content_type=content_type)


class StorageServices:
    storage_prefix = None

    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        self.storage_prefix = f"{self.dataset_id}/"
        self.upload_to_GCP_log = []

    def process(self, valid_data: dict, invalid_data: list, validation_logs: list, forced_replace: bool = False):
        is_new_version_needed = False
        for key, value in valid_data.items():
            if value:
                if not self.check_if_same_file(table_name=key, local_data_list=value) or forced_replace:
                    self.save_to_storage(table_name=key, data=value)
                    is_new_version_needed = True
        self.delete_unmatched_json_files(valid_data=valid_data)

        if invalid_data:
            if not self.check_if_same_file(table_name="invalid_data", local_data_list=invalid_data) or forced_replace:
                self.save_to_storage(table_name="invalid_data", data=invalid_data)
                is_new_version_needed = True
        else:
            self.check_and_delete_single_table(table_name="invalid_data")

        if is_new_version_needed:
            [dct.update({'date_time': datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d-%H-%M-%S UTC"),
                         'api_version': settings.config['VERSION']}) for dct in validation_logs]
            self.save_to_storage(table_name="validation_results", data=validation_logs)

        return is_new_version_needed

    def check_if_same_file(self, table_name, local_data_list):
        blob = gcp_bucket.blob(f"{self.dataset_id}/{table_name}.json")

        if not blob.exists():
            self.upload_to_GCP_log.append(f"creating_{self.dataset_id}/{table_name}.json")
            return False

        # Download the JSON content from GCS into memory as a string
        gcs_json_string = blob.download_as_text()
        gcs_json_data = json.loads(gcs_json_string)

        # Compare the GCS JSON data with the local data list using DeepDiff
        # diff = DeepDiff(gcs_json_data, local_data_list, ignore_order=True)
        # if table_name == 'tasks' or table_name == 'users':
        #     print(diff)
        is_same_length = len(gcs_json_data) == len(local_data_list)

        # If diff is empty, there are no differences
        self.upload_to_GCP_log.append(
            f"{table_name}(gcs/local)_same_length?: {len(gcs_json_data)}/{len(local_data_list)}, {is_same_length}")
        return is_same_length

    def save_to_storage(self, table_name: str, data):
        data_json = json.dumps(data, cls=CustomJSONEncoder)
        destination_blob_name = f"{self.dataset_id}/{table_name}.json"
        try:
            upload_blob_from_memory(data=data_json,
                                    destination_blob_name=destination_blob_name,
                                    content_type='application/json')
            self.upload_to_GCP_log.append(f"Data uploaded to {destination_blob_name}.")
        except Exception as e:
            self.upload_to_GCP_log.append(
                f"Failed to save data to cloud, {self.dataset_id}, {table_name}, {e}")

    def append_list_to_json_in_gcp(self, data: dict, file_name: str):
        # Initialize the GCP Storage client
        blob = gcp_bucket.blob(f"{self.dataset_id}/{file_name}.json")

        # Try to download the existing JSON file
        try:
            content = blob.download_as_text()
            existing_data = json.loads(content)
            if not isinstance(existing_data, list):
                existing_data = []
        except Exception as e:
            logging.info(f"Can't read the file or file not exists, try creating...")
            existing_data = []

        # Append the new data
        existing_data.append(data)
        try:
            # Write back to the JSON file
            blob.upload_from_string(data=json.dumps(existing_data, cls=CustomJSONEncoder),
                                    content_type='application/json')
            logging.info(f"Save to daily_log file.")
        except Exception as e:
            logging.info(f"Failed to save to daily_log file: {e}")

    def list_blobs_with_prefix(self, delimiter=None):
        """Lists all the blobs in the bucket that begin with the prefix."""
        blobs = storage_client.list_blobs(settings.config['CORE_DATA_BUCKET_NAME'], prefix=self.storage_prefix,
                                          delimiter=delimiter)

        return [blob.name for blob in blobs if 'logs' not in blob.name]

    def list_table_names_in_blob(self):
        table_names = self.list_blobs_with_prefix()
        return [name.split('/')[-1].split('.')[0] for name in table_names]

    def check_and_delete_single_table(self, table_name):
        """Deletes a blob from the bucket."""
        blob = gcp_bucket.blob(f"{self.dataset_id}/{table_name}.json")
        if blob.exists():
            blob.delete()
            logging.info(f"Blob {self.dataset_id}/{table_name} deleted.")

    def delete_unmatched_json_files(self, valid_data):
        # List all blobs in the specified bucket and folder
        blobs = gcp_bucket.list_blobs(prefix=self.storage_prefix)

        # Iterate through each blob in the folder
        for blob in blobs:
            # Extract the file name from the blob's name
            file_name = blob.name.split('/')[-1]

            # Check if the file is a .json file and not in the valid keys and does not contain 'log' or 'result'
            if file_name.endswith('.json') and not any(substring in file_name for substring in ['log', 'result', 'invalid']):
                # Extract the key from the file name (assuming format 'xxx.json')
                key = file_name.split('.')[0]

                # Check if the key is not in the dictionary's keys
                if key not in valid_data:
                    # Delete the file
                    blob.delete()
                    logging.info(f"Deleted {file_name} from bucket.")


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)
