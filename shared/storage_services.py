from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY
import json
import os
import logging
import time

import settings
from shared import utils

logging.basicConfig(level=logging.INFO)

# Redivis ingests each table from its GCS blob, so a silently dropped write would
# publish stale tables. Writes are retried and then size-verified before counting.
UPLOAD_ATTEMPTS = 3
UPLOAD_BACKOFF_SECONDS = 2
UPLOAD_TIMEOUT_SECONDS = 300


class StorageServices:
    storage_prefix = None

    def __init__(self, cred, dataset_id: str, is_forced_uploading_redivis: bool = False):
        self.storage_client = storage.Client(credentials=cred)
        # Use the canonical bucket name resolved at startup by utils.setup_project_environment().
        self.gcp_bucket = self.storage_client.bucket(settings.config['CORE_DATA_BUCKET_NAME'])
        self.dataset_id = dataset_id
        self.storage_prefix = f"{self.dataset_id}/"
        self.is_new_version_needed = is_forced_uploading_redivis
        self.upload_to_GCP_log = {
            'new_version_needed': False,
            'blob_file_counts': 0,
            'file_updated': [],
            'file_updated_details': [],
            'file_uploads_fail': [],
            'file_deletion': [],
        }

    @property
    def has_upload_failures(self) -> bool:
        return bool(self.upload_to_GCP_log['file_uploads_fail'])

    def process(self, validated_data: dict):
        for table_name, data in validated_data.items():
            if data and (not self.check_if_same_file(table_name=table_name,
                                                     local_data_list=data) or self.is_new_version_needed):
                if self.save_to_storage(table_name=table_name, data=data):
                    self.is_new_version_needed = True

        self.delete_unmatched_json_files(data=validated_data)
        self.upload_to_GCP_log['new_version_needed'] = self.is_new_version_needed
        self.upload_to_GCP_log['blob_file_counts'] = len(self.list_table_names_in_blob())

    def upload_blob_from_memory(self, data, destination_blob_name, content_type):
        """
            Uploads a file from memory to Google Cloud Storage.

            Args:
            - bucket_name (str): Name of the GCS bucket.
            - data (bytes or str): Data to upload.
            - destination_blob_name (str): Desired name for the file in the bucket.
            - content_type (str): Content type of the file (e.g., 'application/json', 'text/csv').
            """
        # Create a blob object
        blob = self.gcp_bucket.blob(destination_blob_name)

        # Upload the file
        blob.upload_from_string(
            data,
            content_type=content_type,
            timeout=UPLOAD_TIMEOUT_SECONDS,
            retry=DEFAULT_RETRY,
        )

    def get_blob_size(self, destination_blob_name: str):
        """Byte size of the stored blob, read fresh from GCS. None when missing."""
        blob = self.gcp_bucket.get_blob(destination_blob_name)
        return blob.size if blob is not None else None

    def check_if_same_file(self, table_name, local_data_list):
        blob = self.gcp_bucket.blob(f"{self.dataset_id}/{table_name}.json")
        local_count = len(local_data_list)

        if not blob.exists():
            logging.info(f"creating_{self.dataset_id}/{table_name}.json")
            self.upload_to_GCP_log['file_updated'].append(
                f"{table_name}(gcs/local): new/{local_count}")
            self.upload_to_GCP_log['file_updated_details'].append({
                'table': table_name,
                'before': None,
                'after': local_count,
                'delta': local_count,
                'is_new': True,
            })
            return False

        # Download the JSON content from GCS into memory as a string
        gcs_json_string = blob.download_as_text()
        gcs_json_data = json.loads(gcs_json_string)
        gcs_count = len(gcs_json_data)

        is_same_length = gcs_count == local_count
        if not is_same_length:
            self.upload_to_GCP_log['file_updated'].append(
                f"{table_name}(gcs/local): {gcs_count}/{local_count}")
            self.upload_to_GCP_log['file_updated_details'].append({
                'table': table_name,
                'before': gcs_count,
                'after': local_count,
                'delta': local_count - gcs_count,
                'is_new': False,
            })
        return is_same_length

    def save_to_storage(self, table_name: str, data) -> bool:
        """Write one table to GCS. True only when the stored blob matches what we sent."""
        data_json = json.dumps(data, cls=utils.CustomJSONEncoder)
        destination_blob_name = f"{self.dataset_id}/{table_name}.json"
        expected_size = len(data_json.encode('utf-8'))
        last_error = None

        for attempt in range(1, UPLOAD_ATTEMPTS + 1):
            try:
                self.upload_blob_from_memory(data=data_json,
                                        destination_blob_name=destination_blob_name,
                                        content_type='application/json')
                stored_size = self.get_blob_size(destination_blob_name)
                if stored_size != expected_size:
                    raise IOError(
                        f"stored {stored_size} bytes, expected {expected_size}"
                    )
                logging.info(f"{destination_blob_name} uploaded ({expected_size} bytes).")
                return True
            except Exception as e:
                last_error = e
                logging.warning(
                    "GCS upload of %s failed (attempt %s/%s): %s",
                    destination_blob_name, attempt, UPLOAD_ATTEMPTS, e,
                )
                if attempt < UPLOAD_ATTEMPTS:
                    time.sleep(UPLOAD_BACKOFF_SECONDS * (2 ** (attempt - 1)))

        logging.error("GCS upload of %s gave up: %s", destination_blob_name, last_error)
        self.upload_to_GCP_log['file_uploads_fail'].append(f"{table_name}, {last_error}")
        return False

    def append_list_to_json_in_gcp(self, data: dict, file_name: str):
        # Initialize the GCP Storage client
        blob = self.gcp_bucket.blob(f"{self.dataset_id}/{file_name}.json")

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
            blob.upload_from_string(data=json.dumps(existing_data, cls=utils.CustomJSONEncoder),
                                    content_type='application/json')
            logging.info(f"Save to daily_log file.")
        except Exception as e:
            logging.info(f"Failed to save to daily_log file: {e}")

    def list_blobs_with_prefix(self, delimiter=None):
        """Lists all the blobs in the bucket that begin with the prefix."""
        blobs = self.storage_client.list_blobs(settings.config['CORE_DATA_BUCKET_NAME'], prefix=self.storage_prefix,
                                          delimiter=delimiter)

        return [blob.name for blob in blobs if 'logs' not in blob.name]

    def list_table_names_in_blob(self):
        table_names = self.list_blobs_with_prefix()
        return [name.split('/')[-1].split('.')[0] for name in table_names]

    def delete_unmatched_json_files(self, data):
        # List all blobs in the specified bucket and folder
        blobs = self.gcp_bucket.list_blobs(prefix=self.storage_prefix)

        # Iterate through each blob in the folder
        for blob in blobs:
            # Extract the file name from the blob's name
            file_name = blob.name.split('/')[-1]
            # Check if the file is a .json file
            if file_name.endswith('.json'):
                # Extract the key from the file name (assuming format 'xxx.json')
                key = file_name.split('.')[0]
                # Check if the key is not in the dictionary's keys
                if key not in data:
                    # Delete the file
                    try:
                        blob.delete()
                        logging.info(f'{file_name}_deleted_from_{self.dataset_id}')
                        self.upload_to_GCP_log['file_deletion'].append(f'{file_name}')
                    except Exception as e:
                        logging.info(f'{file_name}_deleted_from_{self.dataset_id}_failed, {str(e)}')
                        self.upload_to_GCP_log['file_deletion'].append(f'{file_name}_failed, {str(e)}')

    def check_and_delete_single_table(self, table_name):
        """Deletes a blob from the bucket."""
        blob = self.gcp_bucket.blob(f"{self.dataset_id}/{table_name}.json")
        if blob.exists():
            blob.delete()
            logging.info(f"Blob {self.dataset_id}/{table_name} deleted.")

