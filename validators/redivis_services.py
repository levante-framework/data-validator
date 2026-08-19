import redivis
import logging
import settings
import os
import time
from shared.secret_services import secret_service
from shared.utils import format_redivis_version_description

logging.basicConfig(level=logging.INFO)

# Redivis notebook job statuses that mean another process_dataset run is in flight.
_NOTEBOOK_BUSY_STATUSES = frozenset(
    {"running", "pending", "queued", "starting", "created"}
)


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

    @staticmethod
    def _is_notebook_busy_error(exc: BaseException) -> bool:
        """True when Redivis rejected the run because the shared notebook is busy."""
        msg = str(exc).lower()
        return "already running" in msg

    @staticmethod
    def _notebook_is_busy(nb) -> bool:
        """True when the notebook reports an in-flight ``currentJob``."""
        try:
            nb.get()
            job = (nb.properties or {}).get("currentJob") or {}
            status = str(job.get("status") or "").strip().lower()
            return bool(status and status in _NOTEBOOK_BUSY_STATUSES)
        except Exception as e:
            logging.warning(
                "run_process_dataset_workflow: could not read notebook "
                "currentJob (continuing): %s",
                e,
            )
            return False

    def _wait_for_notebook_idle(self, nb, *, deadline: float, raw_dataset_id: str) -> bool:
        """
        Poll until the shared notebook has no busy ``currentJob``, or ``deadline``.

        Returns True if idle (or status unreadable), False if still busy at deadline.
        """
        poll = max(5, int(settings.config["REDIVIS_PROCESS_BUSY_POLL_SECONDS"]))
        while True:
            if not self._notebook_is_busy(nb):
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            sleep_for = min(poll, remaining)
            logging.info(
                "run_process_dataset_workflow: notebook busy — waiting %.0fs "
                "before retry for %r (%.0fs left in budget)",
                sleep_for,
                raw_dataset_id,
                remaining,
            )
            time.sleep(sleep_for)

    def _point_workflow_datasource(
        self,
        data_source,
        *,
        source_qualified: str,
        raw_dataset_id: str,
    ) -> str | None:
        """
        Point the shared site datasource at ``raw_dataset_id``.

        Returns an error string on mismatch/failure, else None.
        """
        data_source.update(source_dataset=source_qualified, version="current")
        data_source.get()
        actual_name = self._datasource_source_name(data_source)
        if self._redivis_name_key(actual_name) != self._redivis_name_key(raw_dataset_id):
            return (
                f"workflow datasource source mismatch after update: "
                f"wanted {raw_dataset_id!r}, got {actual_name!r}"
            )
        return None

    def _run_shared_notebook_with_busy_retry(
        self,
        *,
        nb,
        data_source,
        metadata_sources: list,
        source_qualified: str,
        target_qualified: str,
        raw_dataset_id: str,
        notebook_name: str,
    ) -> dict:
        """
        Run the shared process_dataset notebook, waiting/retrying while it is busy.

        Re-points the workflow datasource before each attempt so a concurrent job
        cannot leave us pointed at the wrong site after we waited. After a
        successful ``nb.run``, re-reads the datasource and returns ``ran=False``
        if it no longer matches ``raw_dataset_id`` (point→run is not atomic).
        """
        max_wait = max(
            0, int(settings.config["REDIVIS_PROCESS_BUSY_RETRY_MAX_SECONDS"])
        )
        initial_sleep = max(
            1, int(settings.config["REDIVIS_PROCESS_BUSY_RETRY_INITIAL_SECONDS"])
        )
        max_sleep = max(
            initial_sleep,
            int(settings.config["REDIVIS_PROCESS_BUSY_RETRY_MAX_SLEEP_SECONDS"]),
        )
        deadline = time.monotonic() + max_wait
        attempt = 0
        busy_retries = 0
        next_sleep = initial_sleep
        last_busy_error: str | None = None

        while True:
            attempt += 1
            if not self._wait_for_notebook_idle(
                nb, deadline=deadline, raw_dataset_id=raw_dataset_id
            ):
                return {
                    "ran": False,
                    "error": (
                        "shared process_dataset notebook stayed busy for "
                        f"{max_wait}s (last error: {last_busy_error or 'currentJob active'})"
                    ),
                    "busy_retries": busy_retries,
                    "attempts": attempt,
                }

            # Another job may have swapped the datasource while we waited.
            point_err = self._point_workflow_datasource(
                data_source,
                source_qualified=source_qualified,
                raw_dataset_id=raw_dataset_id,
            )
            if point_err:
                return {
                    "ran": False,
                    "error": point_err,
                    "busy_retries": busy_retries,
                    "attempts": attempt,
                }

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
                "(source=%s target=%s attempt=%s busy_retries=%s)",
                notebook_name,
                source_qualified,
                target_qualified,
                attempt,
                busy_retries,
            )
            try:
                nb.run(wait_for_finish=True)
                # Point→run is not atomic: another job can re-point the shared
                # datasource after we pointed and before/during our run. If the
                # source no longer matches, do not report ran=True (avoids a
                # false Airtable processed-date stamp for this site).
                try:
                    data_source.get()
                    actual_name = self._datasource_source_name(data_source)
                except Exception as e:
                    return {
                        "ran": False,
                        "error": (
                            "could not re-read workflow datasource after notebook "
                            f"run for {raw_dataset_id!r}: {e}"
                        ),
                        "busy_retries": busy_retries,
                        "attempts": attempt,
                    }
                if self._redivis_name_key(actual_name) != self._redivis_name_key(
                    raw_dataset_id
                ):
                    return {
                        "ran": False,
                        "error": (
                            "workflow datasource drifted during notebook run: "
                            f"wanted {raw_dataset_id!r}, got {actual_name!r} "
                            "(another job may have re-pointed the shared source; "
                            "not stamping Airtable for this site)"
                        ),
                        "busy_retries": busy_retries,
                        "attempts": attempt,
                    }
                return {
                    "ran": True,
                    "error": None,
                    "busy_retries": busy_retries,
                    "attempts": attempt,
                }
            except Exception as e:
                if not self._is_notebook_busy_error(e):
                    return {
                        "ran": False,
                        "error": str(e),
                        "busy_retries": busy_retries,
                        "attempts": attempt,
                    }
                last_busy_error = str(e)
                busy_retries += 1
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return {
                        "ran": False,
                        "error": (
                            "shared process_dataset notebook stayed busy for "
                            f"{max_wait}s (last error: {last_busy_error})"
                        ),
                        "busy_retries": busy_retries,
                        "attempts": attempt,
                    }
                sleep_for = min(next_sleep, remaining, max_sleep)
                logging.info(
                    "run_process_dataset_workflow: %r — notebook already running; "
                    "retry in %.0fs (busy_retries=%s, %.0fs left in budget)",
                    raw_dataset_id,
                    sleep_for,
                    busy_retries,
                    remaining,
                )
                time.sleep(sleep_for)
                next_sleep = min(next_sleep * 2, max_sleep)

    def release_processed_dataset(
        self,
        *,
        processed_id: str,
        raw_dataset_id: str,
    ) -> dict:
        """
        Release the unmarked processed dataset after ``process_dataset`` finishes.

        The shared notebook writes tables into the dataset's unreleased ``next``
        version but does not release it; the validator triggers release here.
        """
        result = {
            "released": False,
            "skipped": False,
            "processed_dataset_id": processed_id,
            "before_version": None,
            "after_version": None,
            "is_released": False,
            "error": None,
        }
        prev_id = self.dataset_id
        try:
            self.set_dataset(dataset_id=processed_id)
            before = self.get_current_dataset_status()
            result["before_version"] = before.get("version_tag")
            if not before.get("exists"):
                result["error"] = f"processed dataset {processed_id!r} does not exist"
                return result

            # Already on a released version with no pending "next" work.
            if before.get("is_released") and before.get("version_tag") not in (
                None,
                "next",
            ):
                result["skipped"] = True
                result["is_released"] = True
                result["after_version"] = before.get("version_tag")
                logging.info(
                    "release_processed_dataset: %r already released at %s — skipped",
                    processed_id,
                    before.get("version_tag"),
                )
                return result

            description = (
                f"Processed companion for {raw_dataset_id}. "
                f"Released by data-validator after process_dataset workflow."
            )
            self.dataset.update(description=description)
            self.dataset.release()
            after = self.get_current_dataset_status()
            result["after_version"] = after.get("version_tag")
            result["is_released"] = bool(after.get("is_released"))
            if not result["is_released"]:
                result["error"] = (
                    f"release() returned but {processed_id!r} is still unreleased "
                    f"(version={after.get('version_tag')!r})"
                )
                logging.error("release_processed_dataset: %s", result["error"])
                return result
            result["released"] = True
            logging.info(
                "release_processed_dataset: released %r %s → %s",
                processed_id,
                result["before_version"],
                result["after_version"],
            )
        except Exception as e:
            result["error"] = str(e)
            logging.error(
                "release_processed_dataset(%r) failed: %s", processed_id, e
            )
        finally:
            if prev_id:
                self.set_dataset(dataset_id=prev_id)
        return result

    def run_process_dataset_workflow(self, raw_dataset_id: str) -> dict:
        """
        Run Levante ``process_dataset`` for one site, driven only by ``raw_dataset_id``.

        - **Source** (workflow datasource): ``levante.{raw_dataset_id}``
          (must end with ``-raw``, e.g. ``TEST-Ethan-de-pilot-raw``)
        - **Target** (notebook output dataset): unmarked ``{Name}`` derived by
          stripping ``-raw`` (e.g. ``TEST-Ethan-de-pilot``). Ensured to exist
          (create-if-missing) before the notebook runs.
        - After the notebook completes successfully, **release** the processed
          dataset (the notebook leaves an unreleased ``next`` version).

        The shared workflow may still point at a previous site; this method always
        replaces the site (non-metadata) datasource with ``raw_dataset_id`` first,
        then runs the notebook. If another job holds the shared notebook, waits and
        retries (re-pointing the datasource before each attempt) until the budget
        in ``REDIVIS_PROCESS_BUSY_RETRY_MAX_SECONDS`` is exhausted.
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
            "processed_release": None,
            "error": None,
            "busy_retries": 0,
            "attempts": 0,
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

            nb = wf.notebook(notebook_name)
            run_result = self._run_shared_notebook_with_busy_retry(
                nb=nb,
                data_source=data_source,
                metadata_sources=metadata_sources,
                source_qualified=source_qualified,
                target_qualified=target_qualified,
                raw_dataset_id=raw_dataset_id,
                notebook_name=notebook_name,
            )
            result["busy_retries"] = run_result.get("busy_retries", 0)
            result["attempts"] = run_result.get("attempts", 0)
            if run_result.get("error"):
                result["error"] = run_result["error"]
                logging.error(
                    "run_process_dataset_workflow(%r) failed: %s",
                    raw_dataset_id,
                    result["error"],
                )
                return result

            result["ran"] = True
            logging.info(
                "run_process_dataset_workflow: notebook completed source=%s "
                "target=%s (attempts=%s busy_retries=%s) — releasing processed",
                source_qualified,
                target_qualified,
                result["attempts"],
                result["busy_retries"],
            )

            release_log = self.release_processed_dataset(
                processed_id=processed_id,
                raw_dataset_id=raw_dataset_id,
            )
            result["processed_release"] = release_log
            if release_log.get("error"):
                result["error"] = (
                    f"process_dataset notebook completed but processed release "
                    f"failed: {release_log['error']}"
                )
                logging.error(
                    "run_process_dataset_workflow(%r): %s",
                    raw_dataset_id,
                    result["error"],
                )
                return result

            logging.info(
                "run_process_dataset_workflow: completed source=%s target=%s "
                "processed_release=%s",
                source_qualified,
                target_qualified,
                release_log.get("after_version") or release_log.get("before_version"),
            )
        except Exception as e:
            result["error"] = str(e)
            logging.error(
                "run_process_dataset_workflow(%r) failed: %s", raw_dataset_id, e
            )
        return result
