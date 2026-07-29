import redivis
import logging
import settings
import os
from shared.secret_services import secret_service
from shared.utils import format_redivis_version_description

logging.basicConfig(level=logging.INFO)


class RedivisServices:
    dataset = None
    dataset_id = None

    def __init__(self):
        self.organization = redivis.organization(settings.config['INSTANCE'])
        self.upload_to_redivis_log = {
            'table_counts': 0,
            'table_deletions': [],
            'upload_fails': [],
            'dataset_fails': []
        }
        os.environ["REDIVIS_API_TOKEN"] = secret_service.get_secret_payload(
            secret_id=settings.config['REDIVIS_API_TOKEN_SECRET_ID'],
            version_id="latest")
        os.environ['REDIVIS_IDENTITY'] = secret_service.get_secret_payload(
            secret_id=settings.config['REDIVIS_IDENTITY_ACCOUNT_SECRET_ID'],
            version_id="latest")

    def set_dataset(self, dataset_id: str):
        self.dataset_id = dataset_id
        self.dataset = self.organization.dataset(name=dataset_id)

    def get_properties(self):
        properties = self.dataset.get().properties
        properties_value = {
            'is_released': properties.get("version", {}).get("isReleased", None),
            'is_deleted': properties.get("version", {}).get("isDeleted", None),
            'version': properties.get("version", {}).get("tag", None)
        }
        logging.info(properties_value)

    def save_to_redivis_table(self, file_name: str, upload_merge_strategy: str = 'replace'):
        upload_name = file_name.split("/")[1]
        table_name = upload_name.split(".")[0]
        if self.dataset.table(table_name).exists():
            table = self.dataset.table(table_name)
            table.update(upload_merge_strategy=upload_merge_strategy, description=f"This upload is from {file_name}")
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
                    "identity": os.getenv('REDIVIS_IDENTITY'),  # The email associated with the data source
                },
                replace_on_conflict=True,
                remove_on_fail=True,
                raise_on_fail=False
            )
            logging.info(f"{file_name} has been uploaded to redivis table")
        except Exception as e:
            self.upload_to_redivis_log['upload_fails'].append(f"{file_name}_failed, {e}")
            logging.info(f"{file_name} failed to upload to redivis table, {e}")

    def create_dateset_version(self, params: list):
        try:
            if self.dataset.exists():
                self.dataset = self.dataset.create_next_version(if_not_exists=True)
            else:
                description = format_redivis_version_description(
                    {"dataset_id": self.dataset_id, "orgs": params},
                    dataset_id=self.dataset_id,
                )
                self.dataset.create(
                    description=description,
                    public_access_level="overview",
                )
        except Exception as e:
            logging.info(f"Failed on create_dateset_version: {e}")
            self.upload_to_redivis_log['dataset_fails'].append(f"create_dateset_version: {e}")

    def create_empty_dataset_if_missing(self, *, description: str | None = None) -> dict:
        """
        Idempotently create an empty Redivis dataset using ``self.dataset_id``.

        Returns ``{created, already_exists, error}``. The dataset is left unreleased
        with no tables — only the shell exists. ``set_dataset(dataset_id=...)`` must
        be called first.
        """
        result = {"created": False, "already_exists": False, "error": None}
        if self.dataset is None:
            result["error"] = "set_dataset() not called before create_empty_dataset_if_missing()"
            logging.error("create_empty_dataset_if_missing: %s", result["error"])
            return result
        try:
            if self.dataset.exists():
                result["already_exists"] = True
                logging.info(
                    "create_empty_dataset_if_missing: %r already exists — skipped",
                    self.dataset_id,
                )
                return result
            self.dataset.create(
                description=description
                or f"Empty dataset created via data-validator for {self.dataset_id}",
                public_access_level="overview",
            )
            result["created"] = True
            logging.info(
                "create_empty_dataset_if_missing: created empty dataset %r",
                self.dataset_id,
            )
        except Exception as e:
            logging.error(
                "create_empty_dataset_if_missing(%r) failed: %s", self.dataset_id, e
            )
            result["error"] = str(e)
        return result

    def get_reference_id(self) -> str | None:
        """Return the dataset's persistent 4-char ``referenceId`` after ``set_dataset``."""
        try:
            if self.dataset is None or not self.dataset.exists():
                return None
            self.dataset.get()
            props = self.dataset.properties or {}
            ref = props.get("referenceId")
            return str(ref) if ref else None
        except Exception as e:
            logging.info("get_reference_id failed for %r: %s", self.dataset_id, e)
            return None

    def rename_dataset(self, new_name: str) -> dict:
        """
        Rename the dataset currently selected via ``set_dataset``.

        Returns ``{renamed, already_target, error, reference_id}``.
        """
        result = {
            "renamed": False,
            "already_target": False,
            "error": None,
            "reference_id": None,
        }
        if self.dataset is None:
            result["error"] = "set_dataset() not called before rename_dataset()"
            return result
        new_name = (new_name or "").strip()
        if not new_name:
            result["error"] = "new_name is empty"
            return result
        if self.dataset_id == new_name:
            result["already_target"] = True
            result["reference_id"] = self.get_reference_id()
            return result
        try:
            if not self.dataset.exists():
                result["error"] = f"source dataset {self.dataset_id!r} does not exist"
                return result
            self.dataset.update(name=new_name)
            result["renamed"] = True
            self.set_dataset(dataset_id=new_name)
            result["reference_id"] = self.get_reference_id()
            logging.info(
                "rename_dataset: renamed to %r referenceId=%s",
                new_name,
                result["reference_id"],
            )
        except Exception as e:
            logging.error(
                "rename_dataset(%r -> %r) failed: %s", self.dataset_id, new_name, e
            )
            result["error"] = str(e)
        return result

    def release_dataset(self, params: dict):
        try:
            description = format_redivis_version_description(
                params, dataset_id=self.dataset_id
            )
            self.dataset.update(description=description)
            self.dataset.release()
        except Exception as e:
            self.upload_to_redivis_log['dataset_fails'].append(f"release_dataset: {e}")
            logging.info(f"Failed on release_dataset: {e}")

    def count_tables(self):
        return len(self.dataset.list_tables())

    def get_tables(self, table_name: str):
        table = self.dataset.table(table_name)
        df = table.to_pandas_dataframe()
        result = df.to_dict(orient='records')
        return result

    def get_datasets_list(self):
        return [dn.name for dn in self.organization.list_datasets()]

    def is_current_dataset_released(self) -> bool:
        """True if this dataset exists and its current version is released on Redivis."""
        st = self.get_current_dataset_status()
        return bool(st.get("exists") and st.get("is_released"))

    def get_current_dataset_status(self) -> dict:
        """After set_dataset(): whether the dataset exists on Redivis and release metadata."""
        try:
            if self.dataset is None or not self.dataset.exists():
                return {
                    "exists": False,
                    "is_released": False,
                    "version_tag": None,
                    "is_deleted": None,
                }
            props = self.dataset.get().properties or {}
            ver = props.get("version") or {}
            return {
                "exists": True,
                "is_released": bool(ver.get("isReleased", False)),
                "version_tag": ver.get("tag"),
                "is_deleted": ver.get("isDeleted"),
            }
        except Exception as e:
            logging.info(f"get_current_dataset_status failed: {e}")
            return {
                "exists": False,
                "is_released": False,
                "version_tag": None,
                "is_deleted": None,
            }

    def delete_table(self, table_name: str):
        try:
            if self.dataset.table(table_name).exists():
                self.dataset.table(table_name).delete()
                self.upload_to_redivis_log['table_deletions'].append(f"{table_name}_removed")
        except Exception as e:
            self.upload_to_redivis_log['table_deletions'].append(f"{table_name}_removed_failed, {e}")

    @staticmethod
    def processed_name_from_raw(raw_dataset_id: str) -> str:
        """Map ``{Name}-raw`` → unmarked processed ``{Name}``."""
        suffix = settings.config["RAW_DATASET_SUFFIX"]
        raw_dataset_id = (raw_dataset_id or "").strip()
        if raw_dataset_id.endswith(suffix):
            return raw_dataset_id[: -len(suffix)]
        return raw_dataset_id

    @staticmethod
    def _redivis_name_key(name: str) -> str:
        """Normalize Redivis dataset names for comparison (hyphen ↔ underscore)."""
        return (name or "").strip().lower().replace("-", "_")

    @staticmethod
    def _datasource_source_name(ds) -> str:
        props = ds.properties or {}
        source = props.get("sourceDataset") or {}
        if not isinstance(source, dict):
            return ""
        return (
            source.get("name")
            or (source.get("qualifiedReference") or "").split(".")[-1].split(":")[0]
            or ""
        )

    def _ensure_processed_shell(self, *, processed_id: str, raw_id: str) -> dict:
        """Create unmarked processed dataset if missing; no-op if it already exists."""
        prev_id = self.dataset_id
        try:
            self.set_dataset(dataset_id=processed_id)
            return self.create_empty_dataset_if_missing(
                description=(
                    f"Processed companion for {raw_id} "
                    "(created before process_dataset workflow)"
                ),
            )
        finally:
            if prev_id:
                self.set_dataset(dataset_id=prev_id)

    def run_process_dataset_workflow(self, raw_dataset_id: str) -> dict:
        """
        Run Levante ``process_dataset`` for one site, driven only by ``raw_dataset_id``.

        - **Source** (workflow datasource): ``levante.{raw_dataset_id}``
          (must end with ``-raw``, e.g. ``TEST-Ethan-de-pilot-raw``)
        - **Target** (notebook output dataset): unmarked ``{Name}`` derived by
          stripping ``-raw`` (e.g. ``TEST-Ethan-de-pilot``). Ensured to exist
          (create-if-missing) before the notebook runs.

        The shared workflow may still point at a previous site; this method always
        replaces the site (non-metadata) datasource with ``raw_dataset_id`` first,
        then runs the notebook.

        Note: concurrent jobs can race on the shared workflow datasource.
        """
        raw_suffix = settings.config["RAW_DATASET_SUFFIX"]
        raw_dataset_id = (raw_dataset_id or "").strip()
        processed_id = self.processed_name_from_raw(raw_dataset_id)
        result = {
            "ran": False,
            "raw_dataset_id": raw_dataset_id,
            "processed_dataset_id": processed_id,
            "source": None,
            "target": None,
            "processed_shell": None,
            "workflow": settings.config["REDIVIS_PROCESS_WORKFLOW_NAME"],
            "notebook": settings.config["REDIVIS_PROCESS_NOTEBOOK_NAME"],
            "error": None,
        }
        if not raw_dataset_id:
            result["error"] = "raw_dataset_id is empty"
            return result
        if not raw_dataset_id.endswith(raw_suffix):
            result["error"] = (
                f"raw_dataset_id must end with {raw_suffix!r}, got {raw_dataset_id!r}"
            )
            return result
        if not processed_id or processed_id == raw_dataset_id:
            result["error"] = (
                f"could not derive unmarked processed name from {raw_dataset_id!r}"
            )
            return result

        org = settings.config["INSTANCE"].lower()
        source_qualified = f"{org}.{raw_dataset_id}"
        target_qualified = f"{org}.{processed_id}"
        result["source"] = source_qualified
        result["target"] = target_qualified

        try:
            # Target shell first so the notebook can open/write unmarked {Name}.
            processed_shell = self._ensure_processed_shell(
                processed_id=processed_id, raw_id=raw_dataset_id
            )
            result["processed_shell"] = processed_shell
            if processed_shell.get("error"):
                result["error"] = (
                    "processed dataset shell not ready: "
                    f"{processed_shell['error']}"
                )
                return result

            user_name = settings.config["REDIVIS_PROCESS_WORKFLOW_USER"]
            workflow_name = settings.config["REDIVIS_PROCESS_WORKFLOW_NAME"]
            notebook_name = settings.config["REDIVIS_PROCESS_NOTEBOOK_NAME"]
            wf = redivis.user(user_name).workflow(workflow_name)
            datasources = wf.list_datasources()

            site_candidates = []
            metadata_sources = []
            for ds in datasources:
                ds.get()
                source_name = self._datasource_source_name(ds)
                key = self._redivis_name_key(source_name)
                if "metadata" in key:
                    metadata_sources.append(ds)
                else:
                    site_candidates.append((ds, source_name, key))

            # Prefer the datasource already on a *-raw site dataset; else first
            # non-metadata datasource (shared workflow only has one site source).
            data_source = None
            prev_name = ""
            for ds, source_name, key in site_candidates:
                if key.endswith("_raw"):
                    data_source = ds
                    prev_name = source_name
                    break
            if data_source is None and site_candidates:
                data_source, prev_name, _ = site_candidates[0]

            if data_source is None:
                result["error"] = (
                    f"No non-metadata (site) datasource found on workflow "
                    f"{workflow_name!r}"
                )
                logging.error("run_process_dataset_workflow: %s", result["error"])
                return result

            logging.info(
                "run_process_dataset_workflow: source %s → %s ; target %s",
                prev_name or "(unknown)",
                source_qualified,
                target_qualified,
            )
            data_source.update(source_dataset=source_qualified, version="current")
            data_source.get()
            actual_name = self._datasource_source_name(data_source)
            if self._redivis_name_key(actual_name) != self._redivis_name_key(
                raw_dataset_id
            ):
                result["error"] = (
                    f"workflow datasource source mismatch after update: "
                    f"wanted {raw_dataset_id!r}, got {actual_name!r}"
                )
                logging.error("run_process_dataset_workflow: %s", result["error"])
                return result

            for ds in metadata_sources:
                try:
                    ds.update(version="current")
                except Exception as e:
                    logging.warning(
                        "run_process_dataset_workflow: metadata datasource "
                        "version=current refresh failed (continuing): %s",
                        e,
                    )

            logging.info(
                "run_process_dataset_workflow: running notebook %s "
                "(source=%s target=%s)",
                notebook_name,
                source_qualified,
                target_qualified,
            )
            nb = wf.notebook(notebook_name)
            nb.run(wait_for_finish=True)
            result["ran"] = True
            logging.info(
                "run_process_dataset_workflow: completed source=%s target=%s",
                source_qualified,
                target_qualified,
            )
        except Exception as e:
            result["error"] = str(e)
            logging.error(
                "run_process_dataset_workflow(%r) failed: %s", raw_dataset_id, e
            )
        return result
