import redivis
import logging
import settings
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

from google.cloud import firestore as google_firestore

from shared.firestore_services import firestore_services
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
    def _notebook_job_id(job: dict | None) -> str:
        return str((job or {}).get("id") or "").strip()

    @staticmethod
    def _notebook_jobs(nb) -> tuple[dict, dict]:
        props = nb.properties or {}
        current = props.get("currentJob") or {}
        last = props.get("lastRunJob") or {}
        return (
            current if isinstance(current, dict) else {},
            last if isinstance(last, dict) else {},
        )

    def _notebook_job_matching(self, nb, job_id: str) -> dict | None:
        current, last = self._notebook_jobs(nb)
        if self._notebook_job_id(current) == job_id:
            return current
        if self._notebook_job_id(last) == job_id:
            return last
        return None

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

    @staticmethod
    def _lease_doc_id(workflow_name: str) -> str:
        safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", workflow_name or "").strip("_")
        return f"process_dataset_{safe or 'unnamed'}"

    @staticmethod
    def _lease_utcnow() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _lease_expires_at(data: dict | None) -> datetime | None:
        exp = (data or {}).get("expires_at")
        if exp is None:
            return None
        if getattr(exp, "tzinfo", None) is None:
            try:
                return exp.replace(tzinfo=timezone.utc)
            except Exception:
                return None
        return exp

    def _copy_lease_ref(self, workflow_name: str):
        return (
            firestore_services.admin_db.collection("locks").document(
                self._lease_doc_id(workflow_name)
            )
        )

    def _copy_lease_held_by_other(self, workflow_name: str) -> bool:
        """True when another job holds an unexpired lease on this copy."""
        try:
            snap = self._copy_lease_ref(workflow_name).get()
        except Exception as e:
            logging.warning(
                "run_process_dataset_workflow: lease read failed for %s "
                "(treating as held): %s",
                workflow_name,
                e,
            )
            return True
        if not snap.exists:
            return False
        data = snap.to_dict() or {}
        exp = self._lease_expires_at(data)
        now = self._lease_utcnow()
        return bool(data.get("owner") and exp and exp > now)

    def _acquire_copy_lease(
        self,
        workflow_name: str,
        *,
        owner_token: str,
        raw_dataset_id: str,
    ) -> bool:
        """
        Transactionally take the copy lease if free or expired.

        Fail closed on Firestore errors (do not point without a lease).
        """
        doc_ref = self._copy_lease_ref(workflow_name)
        ttl = max(60, int(settings.config.get("REDIVIS_PROCESS_LEASE_TTL_SECONDS", 1800)))

        @google_firestore.transactional
        def _claim(transaction) -> bool:
            snap = doc_ref.get(transaction=transaction)
            now = self._lease_utcnow()
            if snap.exists:
                data = snap.to_dict() or {}
                exp = self._lease_expires_at(data)
                owner = data.get("owner")
                if owner and owner != owner_token and exp and exp > now:
                    return False
            transaction.set(
                doc_ref,
                {
                    "owner": owner_token,
                    "workflow": workflow_name,
                    "raw_dataset_id": raw_dataset_id,
                    "expires_at": now + timedelta(seconds=ttl),
                    "updated_at": now,
                },
            )
            return True

        try:
            ok = bool(_claim(firestore_services.admin_db.transaction()))
        except Exception as e:
            logging.warning(
                "run_process_dataset_workflow: lease acquire failed for %s "
                "(fail closed): %s",
                workflow_name,
                e,
            )
            return False
        if ok:
            logging.info(
                "run_process_dataset_workflow: acquired lease %s for %r",
                workflow_name,
                raw_dataset_id,
            )
        return ok

    def _heartbeat_copy_lease(
        self, workflow_name: str, *, owner_token: str
    ) -> None:
        doc_ref = self._copy_lease_ref(workflow_name)
        ttl = max(60, int(settings.config.get("REDIVIS_PROCESS_LEASE_TTL_SECONDS", 1800)))

        @google_firestore.transactional
        def _beat(transaction) -> bool:
            snap = doc_ref.get(transaction=transaction)
            if not snap.exists:
                return False
            data = snap.to_dict() or {}
            if data.get("owner") != owner_token:
                return False
            now = self._lease_utcnow()
            transaction.update(
                doc_ref,
                {
                    "expires_at": now + timedelta(seconds=ttl),
                    "updated_at": now,
                },
            )
            return True

        try:
            if not _beat(firestore_services.admin_db.transaction()):
                logging.warning(
                    "run_process_dataset_workflow: lease heartbeat skipped "
                    "for %s (no longer owner)",
                    workflow_name,
                )
        except Exception as e:
            logging.warning(
                "run_process_dataset_workflow: lease heartbeat failed for %s: %s",
                workflow_name,
                e,
            )

    def _release_copy_lease(
        self, workflow_name: str, *, owner_token: str
    ) -> None:
        doc_ref = self._copy_lease_ref(workflow_name)

        @google_firestore.transactional
        def _drop(transaction) -> None:
            snap = doc_ref.get(transaction=transaction)
            if not snap.exists:
                return
            data = snap.to_dict() or {}
            if data.get("owner") != owner_token:
                return
            transaction.delete(doc_ref)

        try:
            _drop(firestore_services.admin_db.transaction())
            logging.info(
                "run_process_dataset_workflow: released lease %s",
                workflow_name,
            )
        except Exception as e:
            logging.warning(
                "run_process_dataset_workflow: lease release failed for %s: %s",
                workflow_name,
                e,
            )

    def _wait_for_notebook_job(
        self,
        nb,
        *,
        job_id: str,
        raw_dataset_id: str,
        on_poll=None,
    ) -> str | None:
        """
        Poll until the started notebook ``job_id`` completes or fails.

        Do not follow whatever ``currentJob`` is live: another site can start
        the shared notebook after ours finishes, and ``wait_for_finish=True``
        would then wait on *their* job.
        """
        poll = 2
        while True:
            if on_poll is not None:
                try:
                    on_poll()
                except Exception as e:
                    logging.warning(
                        "run_process_dataset_workflow: wait poll hook failed "
                        "for %r: %s",
                        raw_dataset_id,
                        e,
                    )
            nb.get()
            ours = self._notebook_job_matching(nb, job_id)
            if ours is None:
                current, last = self._notebook_jobs(nb)
                logging.info(
                    "run_process_dataset_workflow: waiting for notebook job %s "
                    "for %r (current=%s last=%s)",
                    job_id,
                    raw_dataset_id,
                    self._notebook_job_id(current) or "none",
                    self._notebook_job_id(last) or "none",
                )
                time.sleep(poll)
                continue
            status = str(ours.get("status") or "").strip().lower()
            if status == "completed":
                return None
            if status == "failed":
                return (
                    ours.get("errorMessage")
                    or f"notebook job {job_id} failed"
                )
            time.sleep(poll)

    @staticmethod
    def _process_workflow_pool() -> list[str]:
        """Qualified workflow refs to idle-claim, NAME as fallback if the pool is empty."""
        names: list[str] = []
        seen: set[str] = set()
        for raw in settings.config.get("REDIVIS_PROCESS_WORKFLOW_POOL") or []:
            name = str(raw or "").strip()
            if not name or name.startswith("#") or name in seen:
                continue
            seen.add(name)
            names.append(name)
        fallback = str(
            settings.config.get("REDIVIS_PROCESS_WORKFLOW_NAME") or ""
        ).strip()
        if fallback and fallback not in seen:
            names.append(fallback)
        return names

    def _open_workflow_slot(self, workflow_name: str, notebook_name: str) -> dict:
        """
        Resolve notebook + site/metadata datasources for one pool workflow.

        Returns a slot dict, or ``{error: ...}`` if the copy cannot be used.
        """
        wf = redivis.organization(settings.config["INSTANCE"]).workflow(workflow_name)
        try:
            if not wf.exists():
                return {"error": f"workflow {workflow_name!r} does not exist"}
        except Exception as e:
            return {"error": f"workflow {workflow_name!r} lookup failed: {e}"}

        site_candidates = []
        metadata_sources = []
        for ds in wf.list_datasources():
            ds.get()
            source_ds = (ds.properties or {}).get("sourceDataset") or {}
            if not isinstance(source_ds, dict) or not source_ds.get("name"):
                continue
            source_name = self._datasource_source_name(ds)
            key = self._redivis_name_key(source_name)
            if "metadata" in key:
                metadata_sources.append(ds)
            else:
                site_candidates.append((ds, source_name, key))

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
            return {
                "error": (
                    f"No non-metadata (site) datasource found on workflow "
                    f"{workflow_name!r}"
                )
            }

        nb = wf.notebook(notebook_name)
        try:
            if not nb.exists():
                return {
                    "error": (
                        f"notebook {notebook_name!r} missing on workflow "
                        f"{workflow_name!r}"
                    )
                }
        except Exception as e:
            return {
                "error": (
                    f"notebook {notebook_name!r} on {workflow_name!r} "
                    f"lookup failed: {e}"
                )
            }
        return {
            "workflow_name": workflow_name,
            "nb": nb,
            "data_source": data_source,
            "metadata_sources": metadata_sources,
            "prev_name": prev_name,
        }

    def _wait_for_any_pool_idle(
        self, slots: list, *, deadline: float, raw_dataset_id: str
    ) -> bool:
        """
        Poll until any pool copy is claimable (notebook idle and no foreign
        lease), or ``deadline``.

        Returns True if at least one copy is claimable, False if all stay busy.
        """
        poll = max(5, int(settings.config["REDIVIS_PROCESS_BUSY_POLL_SECONDS"]))
        while True:
            idle = [
                s["workflow_name"]
                for s in slots
                if not self._notebook_is_busy(s["nb"])
                and not self._copy_lease_held_by_other(s["workflow_name"])
            ]
            if idle:
                logging.info(
                    "run_process_dataset_workflow: idle copies for %r: %s",
                    raw_dataset_id,
                    idle,
                )
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            sleep_for = min(poll, remaining)
            logging.info(
                "run_process_dataset_workflow: all copies busy — waiting %.0fs "
                "for %r (%.0fs left in budget; pool=%s)",
                sleep_for,
                raw_dataset_id,
                remaining,
                [s["workflow_name"] for s in slots],
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
        slots: list,
        source_qualified: str,
        target_qualified: str,
        raw_dataset_id: str,
        notebook_name: str,
    ) -> dict:
        """
        Idle-claim a process_dataset copy, then run it.

        Picks any idle notebook whose Firestore copy-lease is free. Acquires
        that lease before re-pointing the datasource. If all copies are busy or
        leased, waits until one is free. On ``already running`` or a held
        lease, tries the other copies immediately (no sleep). Wait is bound to
        the job id this start returned. The lease is held until that wait
        finishes (heartbeat while polling) so another job cannot re-point
        mid-run.
        """
        empty = {
            "ran": False,
            "error": None,
            "busy_retries": 0,
            "attempts": 0,
            "workflow": None,
        }
        if not slots:
            empty["error"] = "no process_dataset workflow copies available"
            return empty

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
        last_point_error: str | None = None
        pool_names = [s["workflow_name"] for s in slots]

        def _fail(error: str) -> dict:
            return {
                "ran": False,
                "error": error,
                "busy_retries": busy_retries,
                "attempts": attempt,
                "workflow": None,
            }

        while True:
            attempt += 1
            if not self._wait_for_any_pool_idle(
                slots, deadline=deadline, raw_dataset_id=raw_dataset_id
            ):
                return _fail(
                    "process_dataset copies stayed busy for "
                    f"{max_wait}s (pool={pool_names}; last error: "
                    f"{last_busy_error or 'currentJob active'})"
                )

            idle_slots = [
                s
                for s in slots
                if not self._notebook_is_busy(s["nb"])
                and not self._copy_lease_held_by_other(s["workflow_name"])
            ]
            if not idle_slots:
                continue

            claimed = False
            heartbeat_every = max(
                30,
                int(
                    settings.config.get(
                        "REDIVIS_PROCESS_LEASE_HEARTBEAT_SECONDS", 120
                    )
                ),
            )
            for slot in idle_slots:
                workflow_name = slot["workflow_name"]
                nb = slot["nb"]
                owner_token = uuid.uuid4().hex
                if not self._acquire_copy_lease(
                    workflow_name,
                    owner_token=owner_token,
                    raw_dataset_id=raw_dataset_id,
                ):
                    claimed = True
                    busy_retries += 1
                    last_busy_error = f"{workflow_name} lease held"
                    logging.info(
                        "run_process_dataset_workflow: %r — %s lease held; "
                        "trying next copy immediately (busy_retries=%s)",
                        raw_dataset_id,
                        workflow_name,
                        busy_retries,
                    )
                    continue

                last_beat = time.monotonic()

                def _on_poll(
                    _wf=workflow_name,
                    _tok=owner_token,
                    _every=heartbeat_every,
                ):
                    nonlocal last_beat
                    if time.monotonic() - last_beat < _every:
                        return
                    last_beat = time.monotonic()
                    self._heartbeat_copy_lease(_wf, owner_token=_tok)

                try:
                    if self._notebook_is_busy(nb):
                        claimed = True
                        busy_retries += 1
                        last_busy_error = (
                            f"{workflow_name} became busy after lease"
                        )
                        logging.info(
                            "run_process_dataset_workflow: %r — %s busy after "
                            "lease; releasing and trying next copy",
                            raw_dataset_id,
                            workflow_name,
                        )
                        continue

                    point_err = self._point_workflow_datasource(
                        slot["data_source"],
                        source_qualified=source_qualified,
                        raw_dataset_id=raw_dataset_id,
                    )
                    if point_err:
                        last_point_error = f"{workflow_name}: {point_err}"
                        logging.warning(
                            "run_process_dataset_workflow: skip %s — %s",
                            workflow_name,
                            point_err,
                        )
                        continue

                    for ds in slot["metadata_sources"]:
                        try:
                            ds.update(version="current")
                        except Exception as e:
                            logging.warning(
                                "run_process_dataset_workflow: metadata datasource "
                                "version=current refresh failed on %s (continuing): %s",
                                workflow_name,
                                e,
                            )

                    logging.info(
                        "run_process_dataset_workflow: claiming %s notebook %s "
                        "(source=%s target=%s attempt=%s busy_retries=%s)",
                        workflow_name,
                        notebook_name,
                        source_qualified,
                        target_qualified,
                        attempt,
                        busy_retries,
                    )
                    try:
                        nb.run(wait_for_finish=False)
                    except Exception as e:
                        if not self._is_notebook_busy_error(e):
                            return {
                                "ran": False,
                                "error": f"{workflow_name}: {e}",
                                "busy_retries": busy_retries,
                                "attempts": attempt,
                                "workflow": workflow_name,
                            }
                        claimed = True
                        last_busy_error = str(e)
                        busy_retries += 1
                        logging.info(
                            "run_process_dataset_workflow: %r — %s already "
                            "running; trying next idle copy immediately "
                            "(busy_retries=%s)",
                            raw_dataset_id,
                            workflow_name,
                            busy_retries,
                        )
                        continue

                    current, last = self._notebook_jobs(nb)
                    job_id = self._notebook_job_id(
                        current
                    ) or self._notebook_job_id(last)
                    if not job_id:
                        return {
                            "ran": False,
                            "error": (
                                f"notebook run started on {workflow_name} but "
                                "Redivis returned no job id"
                            ),
                            "busy_retries": busy_retries,
                            "attempts": attempt,
                            "workflow": workflow_name,
                        }
                    logging.info(
                        "run_process_dataset_workflow: started notebook job %s "
                        "on %s for %r (attempt=%s)",
                        job_id,
                        workflow_name,
                        raw_dataset_id,
                        attempt,
                    )
                    wait_err = self._wait_for_notebook_job(
                        nb,
                        job_id=job_id,
                        raw_dataset_id=raw_dataset_id,
                        on_poll=_on_poll,
                    )
                    if wait_err:
                        return {
                            "ran": False,
                            "error": f"{workflow_name}: {wait_err}",
                            "busy_retries": busy_retries,
                            "attempts": attempt,
                            "workflow": workflow_name,
                        }
                    return {
                        "ran": True,
                        "error": None,
                        "busy_retries": busy_retries,
                        "attempts": attempt,
                        "workflow": workflow_name,
                    }
                finally:
                    self._release_copy_lease(
                        workflow_name, owner_token=owner_token
                    )

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return _fail(
                    "process_dataset copies stayed busy for "
                    f"{max_wait}s (pool={pool_names}; last error: "
                    f"{last_busy_error or last_point_error or 'currentJob active'})"
                )
            if not claimed and last_point_error:
                return _fail(last_point_error)
            sleep_for = min(next_sleep, remaining, max_sleep)
            logging.info(
                "run_process_dataset_workflow: %r — no copy claimed; "
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

            # Released current + pending unreleased next is the normal case after
            # the notebook. Skip only when there is no next version to publish.
            props = self.dataset.properties or {}
            pending_next = bool(props.get("nextVersion"))
            already_released_current = bool(before.get("is_released")) and before.get(
                "version_tag"
            ) not in (None, "next")
            if already_released_current and not pending_next:
                result["skipped"] = True
                result["is_released"] = True
                result["after_version"] = before.get("version_tag")
                logging.info(
                    "release_processed_dataset: %r has no unreleased next "
                    "(current=%s) — skipped",
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
            if (
                already_released_current
                and result["after_version"] == result["before_version"]
            ):
                result["error"] = (
                    f"release() returned but processed version_tag stayed "
                    f"{result['after_version']!r} (expected a new tag)"
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

    def run_process_dataset_workflow(
        self,
        raw_dataset_id: str,
        *,
        release_processed: bool = True,
    ) -> dict:
        """
        Run Levante ``process_dataset`` for one site, driven only by ``raw_dataset_id``.

        - **Source** (workflow datasource): ``levante.{raw_dataset_id}``
          (must end with ``-raw``, e.g. ``TEST-Ethan-de-pilot-raw``)
        - **Target** (notebook output dataset): unmarked ``{Name}`` derived by
          stripping ``-raw`` (e.g. ``TEST-Ethan-de-pilot``). Ensured to exist
          (create-if-missing) before the notebook runs.
        - After the notebook completes successfully, **release** the processed
          dataset (the notebook leaves an unreleased ``next`` version) unless
          ``release_processed`` is false.

        The workflow pool (``REDIVIS_PROCESS_WORKFLOW_POOL``) may still point at
        a previous site; this method always replaces the chosen copy's site
        (non-metadata) datasource with ``raw_dataset_id`` first, then runs that
        notebook. Idle copies are claimed first; if all are busy, waits until
        any is free. On ``already running``, tries the other copies immediately.
        Retries until ``REDIVIS_PROCESS_BUSY_RETRY_MAX_SECONDS`` is exhausted.
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
            "workflow_pool": [],
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

            notebook_name = settings.config["REDIVIS_PROCESS_NOTEBOOK_NAME"]
            pool = self._process_workflow_pool()
            result["workflow_pool"] = pool
            if not pool:
                result["error"] = (
                    "REDIVIS_PROCESS_WORKFLOW_POOL / "
                    "REDIVIS_PROCESS_WORKFLOW_NAME is empty"
                )
                logging.error("run_process_dataset_workflow: %s", result["error"])
                return result

            slots = []
            slot_errors = []
            for name in pool:
                slot = self._open_workflow_slot(name, notebook_name)
                if slot.get("error"):
                    slot_errors.append(f"{name}: {slot['error']}")
                    logging.warning(
                        "run_process_dataset_workflow: skipping copy %s — %s",
                        name,
                        slot["error"],
                    )
                    continue
                slots.append(slot)
            if not slots:
                result["error"] = (
                    "No usable process_dataset copies in pool "
                    f"{pool}: {'; '.join(slot_errors) or 'none opened'}"
                )
                logging.error("run_process_dataset_workflow: %s", result["error"])
                return result

            logging.info(
                "run_process_dataset_workflow: pool %s ; target %s ; "
                "copy sources %s",
                [s["workflow_name"] for s in slots],
                target_qualified,
                {
                    s["workflow_name"]: s["prev_name"] or "(unknown)"
                    for s in slots
                },
            )

            run_result = self._run_shared_notebook_with_busy_retry(
                slots=slots,
                source_qualified=source_qualified,
                target_qualified=target_qualified,
                raw_dataset_id=raw_dataset_id,
                notebook_name=notebook_name,
            )
            result["busy_retries"] = run_result.get("busy_retries", 0)
            result["attempts"] = run_result.get("attempts", 0)
            if run_result.get("workflow"):
                result["workflow"] = run_result["workflow"]
            if run_result.get("error"):
                result["error"] = run_result["error"]
                logging.error(
                    "run_process_dataset_workflow(%r) failed: %s",
                    raw_dataset_id,
                    result["error"],
                )
                return result

            result["ran"] = True
            if not release_processed:
                result["processed_release"] = {
                    "released": False,
                    "skipped": True,
                    "skip_reason": "release_processed_dataset=false",
                    "processed_dataset_id": processed_id,
                    "before_version": None,
                    "after_version": None,
                    "is_released": False,
                    "error": None,
                }
                logging.info(
                    "run_process_dataset_workflow: notebook completed on %s "
                    "source=%s target=%s — skipped processed release "
                    "(release_processed_dataset=false)",
                    result["workflow"],
                    source_qualified,
                    target_qualified,
                )
                return result

            logging.info(
                "run_process_dataset_workflow: notebook completed on %s "
                "source=%s target=%s (attempts=%s busy_retries=%s) — "
                "releasing processed",
                result["workflow"],
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
                "run_process_dataset_workflow: completed on %s source=%s "
                "target=%s processed_release=%s",
                result["workflow"],
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
