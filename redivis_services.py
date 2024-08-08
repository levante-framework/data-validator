import redivis
import os
import logging
import settings
from secret_services import secret_services

logging.basicConfig(level=logging.INFO)


class RedivisServices:
    is_released = None
    is_deleted = None
    version = None
    dataset = None
    dataset_id = None

    def __init__(self):
        self.organization = redivis.organization(settings.config['INSTANCE'])
        self.upload_to_redivis_log = []

    def set_dataset(self, dataset_id: str):
        self.dataset_id = dataset_id
        self.dataset = self.organization.dataset(name=dataset_id)

    def get_properties(self):
        properties = self.dataset.get().properties
        self.is_released = properties.get("version", {}).get("isReleased", None)
        self.is_deleted = properties.get("version", {}).get("isDeleted", None)
        self.version = properties.get("version", {}).get("tag", None)

    def save_to_redivis_table(self, file_name: str):
        upload_name = file_name.split("/")[1]
        table_name = upload_name.split(".")[0]
        if self.dataset.table(table_name).exists():
            table = self.dataset.table(table_name)
            table.update(upload_merge_strategy='replace', description=f"This upload is from {file_name}")
        else:
            table = (
                self.dataset
                .table(table_name)
                .create(description=f"{table_name}_table",
                        upload_merge_strategy='replace')
            )
        logging.info(f"Uploading {table_name} to Redivis.")
        upload = table.upload(name=upload_name)
        try:
            upload.create(
                transfer_specification={
                    "sourceType": "gcs",  # one of gcs, s3, bigQuery, url, redivis
                    "sourcePath": f"{settings.config['CORE_DATA_BUCKET_NAME']}/{file_name}",
                    "identity": secret_services.access_secret_version(secret_id=settings.config['REDIVIS_IDENTITY_ACCOUNT_SECRET_ID'],
                                                                      version_id="latest"),  # The email associated with the data source
                },
                replace_on_conflict=True,
                remove_on_fail=True,
                raise_on_fail=False
            )
            self.upload_to_redivis_log.append(f"{file_name} has been uploaded to redivis table")
            logging.info(f"{file_name} has been uploaded to redivis table")
        except Exception as e:
            self.upload_to_redivis_log.append(f"{file_name} failed to upload to redivis table, {e}")
            logging.info(f"{file_name} failed to upload to redivis table, {e}")

    def create_dateset_version(self, params: dict):
        try:
            if self.dataset.exists():
                self.dataset = self.dataset.create_next_version(if_not_exists=True)
            else:
                self.dataset.create(description=f"This is a dataset for {self.dataset_id}, with API params: {params}", public_access_level="overview")
        except Exception as e:
            self.upload_to_redivis_log.append(f"Failed on create_dateset_version: {e}")
            logging.info(f"Failed on create_dateset_version: {e}")

    def release_dataset(self, params: dict):
        try:
            self.dataset.update(description=f"This is a dataset for {self.dataset_id}, current API params: {params}")
            self.dataset.release()
        except Exception as e:
            self.upload_to_redivis_log.append(f"Failed on release_dataset: {e}")
        self.get_properties()

    def count_tables(self):
        return len(self.dataset.list_tables())

    def get_tables(self, table_name: str):
        table = self.dataset.table(table_name)
        df = table.to_pandas_dataframe()
        result = df.to_dict(orient='records')
        return result

    def get_specified_table(self, table_list: list, spec_key: str, spec_value: str):
        return [item for item in table_list if item.get(spec_key) == spec_value]

    def get_datasets_list(self):
        return [dn.name for dn in self.organization.list_datasets()]

    def delete_table(self, table_name: str):
        try:
            if self.dataset.table(table_name).exists():
                self.dataset.table(table_name).delete()
        except Exception as e:
            self.upload_to_redivis_log.append(f"Failed to delete table {table_name}: {e}")
