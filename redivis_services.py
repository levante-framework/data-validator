import redivis
import os
import settings

os.environ["REDIVIS_API_TOKEN"] = settings.redivis_api_token


class RedivisServices:
    is_released = None
    is_deleted = None
    version = None

    def __init__(self, lab_id: str, is_from_firestore: bool, dataset_version):
        self.lab_id = lab_id
        self.source = "firestore" if is_from_firestore else "redivis"
        self.organization = redivis.organization("LEVANTE")
        if dataset_version:
            self.dataset = self.organization.dataset(name=lab_id, version=dataset_version)
        else:
            self.dataset = self.organization.dataset(name=lab_id)
        self.get_properties()

    def get_properties(self):
        properties = self.dataset.get().properties
        self.is_released = properties.get("version", {}).get("isReleased", None)
        self.is_deleted = properties.get("version", {}).get("isDeleted", None)
        self.version = properties.get("version", {}).get("tag", None)
        print(f"Current DS, version:{self.version}, is_released:{self.is_released}, is_deleted:{self.is_deleted}")

    def save_to_redivis_table(self, file_name: str):
        upload_name = file_name.split("/")[1]
        table_name = upload_name.split(".")[0]
        if self.dataset.table(f"{table_name}").exists():
            table = self.dataset.table(f"{table_name}")
            table.update(upload_merge_strategy='replace', description=f"This upload is from {file_name}")
        else:
            table = (
                self.dataset
                .table(f"{table_name}")
                .create(description=f"{table_name}_table from {self.source}",
                        upload_merge_strategy='replace')
            )

        upload = table.upload(name=upload_name)
        try:
            upload.create(
                transfer_specification={
                    "sourceType": "gcs",  # one of gcs, s3, bigQuery, url, redivis
                    "sourcePath": f"{settings.BUCKET_NAME}/{file_name}",
                    "identity": "ezhang61@stanford.edu",  # The email associated with the data source
                },
                replace_on_conflict=True,
                remove_on_fail=True
            )
            print(f"{file_name} has been uploaded to redivis table")
        except Exception as e:
            print(f"{file_name} failed to upload to redivis table, {e}")

    def create_dateset_version(self):
        if self.dataset.exists():
            self.dataset = self.dataset.create_next_version(if_not_exists=True)
        else:
            self.dataset.create(description=f"This is a dataset for {self.lab_id}", public_access_level="overview")

    def release_dataset(self):
        self.dataset.release()
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

