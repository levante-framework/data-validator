"""Cloud Scheduler helpers for per-site daily data-validator Cloud Run Job triggers."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from typing import Any

from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud import scheduler_v1
from google.protobuf import field_mask_pb2

import settings
from shared.run_job_services import build_run_job_request_body, run_job_api_uri

logging.basicConfig(level=logging.INFO)

_SAFE_JOB_ID = re.compile(r"[^a-zA-Z0-9_-]")


def _safe_job_id(suffix: str) -> str:
    """Cloud Scheduler job ids only allow letters, numbers, underscores, hyphens."""
    cleaned = _SAFE_JOB_ID.sub("-", suffix.strip()) if suffix else "unnamed"
    cleaned = cleaned.strip("-_") or "unnamed"
    return cleaned[:500]


def _staggered_minute(
    dataset_id: str, *, window_minutes: int, base_minute: int
) -> int:
    """
    Deterministic minute slot in [base_minute, base_minute + window_minutes).

    SHA-256 of dataset_id is used so the slot is stable across runs (re-running
    redivis_release won't reshuffle existing jobs onto different minutes).
    """
    if window_minutes <= 0:
        return base_minute % 60
    digest = hashlib.sha256(dataset_id.encode("utf-8")).digest()
    offset = int.from_bytes(digest[:4], "big") % window_minutes
    return (base_minute + offset) % 60


def compute_staggered_cron(dataset_id: str) -> str:
    """
    Build the daily-cron schedule for ``dataset_id`` using the stagger settings.
    Same dataset_id always returns the same cron string.
    """
    hour = int(settings.config.get("CLOUD_SCHEDULER_HOUR", 12))
    base_minute = int(settings.config.get("CLOUD_SCHEDULER_BASE_MINUTE", 0))
    window_minutes = int(
        settings.config.get("CLOUD_SCHEDULER_STAGGER_WINDOW_MINUTES", 30)
    )
    minute = _staggered_minute(
        dataset_id,
        window_minutes=window_minutes,
        base_minute=base_minute,
    )
    return f"{minute} {hour} * * *"


class SchedulerServices:
    """Thin wrapper around the Cloud Scheduler v1 API."""

    def __init__(self):
        self._client = scheduler_v1.CloudSchedulerClient()
        self._project_id = os.getenv("project_id")
        if not self._project_id:
            raise RuntimeError(
                "SchedulerServices requires the 'project_id' environment variable."
            )
        self._region = settings.config.get("CLOUD_SCHEDULER_REGION", "us-central1")
        self._timezone = settings.config.get("CLOUD_SCHEDULER_TIMEZONE", "America/Los_Angeles")
        self._job_prefix = settings.config.get("CLOUD_SCHEDULER_JOB_PREFIX", "data-validator")
        self._target_url = run_job_api_uri(project_id=self._project_id, region=self._region)
        self._oauth_service_account = self._resolve_oauth_service_account()
        self._retry_config_dict = {
            "retry_count": int(
                settings.config.get("CLOUD_SCHEDULER_RETRY_COUNT", 3)
            ),
            "max_retry_duration_seconds": int(
                settings.config.get("CLOUD_SCHEDULER_RETRY_MAX_DURATION_SECONDS", 1800)
            ),
            "min_backoff_seconds": int(
                settings.config.get("CLOUD_SCHEDULER_RETRY_MIN_BACKOFF_SECONDS", 60)
            ),
            "max_backoff_seconds": int(
                settings.config.get("CLOUD_SCHEDULER_RETRY_MAX_BACKOFF_SECONDS", 600)
            ),
            "max_doublings": int(
                settings.config.get("CLOUD_SCHEDULER_RETRY_MAX_DOUBLINGS", 3)
            ),
        }
        self._attempt_deadline_seconds = int(
            settings.config.get("CLOUD_SCHEDULER_ATTEMPT_DEADLINE_SECONDS", 1800)
        )

    def _resolve_oauth_service_account(self) -> str:
        configured = (settings.config.get("CLOUD_SCHEDULER_OAUTH_SERVICE_ACCOUNT") or "").strip()
        if configured:
            return configured.format(project_id=self._project_id)
        return f"{self._project_id}@appspot.gserviceaccount.com"

    def _build_retry_config(self) -> "scheduler_v1.RetryConfig":
        cfg = self._retry_config_dict
        return scheduler_v1.RetryConfig(
            retry_count=cfg["retry_count"],
            max_retry_duration={"seconds": cfg["max_retry_duration_seconds"]},
            min_backoff_duration={"seconds": cfg["min_backoff_seconds"]},
            max_backoff_duration={"seconds": cfg["max_backoff_seconds"]},
            max_doublings=cfg["max_doublings"],
        )

    def _build_http_target(self, payload: dict) -> scheduler_v1.HttpTarget:
        return scheduler_v1.HttpTarget(
            uri=self._target_url,
            http_method=scheduler_v1.HttpMethod.POST,
            headers={"Content-Type": "application/json"},
            body=build_run_job_request_body(payload=payload),
            oauth_token=scheduler_v1.OAuthToken(
                service_account_email=self._oauth_service_account,
                scope="https://www.googleapis.com/auth/cloud-platform",
            ),
        )

    @property
    def retry_config_summary(self) -> dict:
        """Read-only snapshot of the retry config that newly-created jobs receive."""
        return dict(self._retry_config_dict)

    @property
    def parent(self) -> str:
        return f"projects/{self._project_id}/locations/{self._region}"

    def job_name(self, dataset_id: str) -> str:
        suffix = _safe_job_id(dataset_id)
        prefix = (self._job_prefix or "").strip("-")
        job_id = f"{prefix}-{suffix}" if prefix else suffix
        return f"{self.parent}/jobs/{job_id}"

    def job_exists(self, dataset_id: str) -> bool:
        try:
            self._client.get_job(name=self.job_name(dataset_id))
            return True
        except NotFound:
            return False

    def _payload_from_http_target(
        self, http: scheduler_v1.HttpTarget | None
    ) -> dict | None:
        """Extract the validation payload from a scheduler HTTP target body."""
        if not http or not http.body:
            return None
        try:
            parsed = json.loads(http.body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
        if not isinstance(parsed, dict):
            return None
        if "overrides" in parsed:
            overrides = parsed.get("overrides") or {}
            containers = overrides.get("containerOverrides") or []
            if not containers:
                return None
            for env in containers[0].get("env") or []:
                if env.get("name") == "DATA_VALIDATOR_PAYLOAD" and env.get("value"):
                    inner = json.loads(env["value"])
                    return inner if isinstance(inner, dict) else None
            return None
        if "dataset_id" in parsed:
            return parsed
        if "operation" in parsed:
            return parsed
        return None

    def _needs_target_migration(self, existing: scheduler_v1.Job) -> bool:
        """True when an existing job still points at the retired Cloud Function."""
        http = existing.http_target
        if not http or not http.uri:
            return True
        uri = http.uri.strip()
        if uri == self._target_url:
            payload = self._payload_from_http_target(http)
            return payload is None
        legacy_markers = (
            "cloudfunctions.net/data-validator",
            "run.app/data-validator",
        )
        return any(marker in uri for marker in legacy_markers)

    def migrate_all_legacy_scheduler_jobs(self) -> dict[str, Any]:
        """
        Scan every Cloud Scheduler job in the configured region and migrate any
        that still target the retired Cloud Function (or use the old request body).
        """
        summary: dict[str, Any] = {
            "scanned": 0,
            "already_current": 0,
            "updated": 0,
            "skipped_no_payload": 0,
            "errors": 0,
            "jobs": [],
        }
        for existing in self._client.list_jobs(parent=self.parent):
            summary["scanned"] += 1
            if not self._needs_target_migration(existing):
                summary["already_current"] += 1
                continue
            payload = self._payload_from_http_target(existing.http_target)
            if not payload:
                summary["skipped_no_payload"] += 1
                job_id = existing.name.rsplit("/", 1)[-1]
                summary["jobs"].append(
                    {"job_id": job_id, "status": "skipped_no_payload"}
                )
                logging.warning(
                    "scheduler: cannot migrate %s — no recognizable payload in body",
                    existing.name,
                )
                continue
            dataset_id = str(payload.get("dataset_id") or existing.name.rsplit("/", 1)[-1])
            result = self._update_validator_job(
                dataset_id=dataset_id,
                payload=payload,
                description=existing.description,
                existing=existing,
            )
            entry = {
                "job_id": existing.name.rsplit("/", 1)[-1],
                "dataset_id": dataset_id,
                "status": "error" if result.get("error") else "updated",
                "error": result.get("error"),
            }
            summary["jobs"].append(entry)
            if result.get("error"):
                summary["errors"] += 1
            elif result.get("updated"):
                summary["updated"] += 1
        logging.info("scheduler: migrate_all_legacy summary=%s", summary)
        return summary

    def _update_validator_job(
        self,
        *,
        dataset_id: str,
        payload: dict,
        description: str | None,
        existing: scheduler_v1.Job,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "created": False,
            "updated": False,
            "already_exists": True,
            "job_name": existing.name,
            "url": self._target_url,
            "error": None,
        }
        if not self._needs_target_migration(existing):
            logging.info(
                "scheduler: job already on Run Job API for dataset_id=%s name=%s",
                dataset_id,
                result["job_name"],
            )
            return result

        cron = existing.schedule or compute_staggered_cron(dataset_id)
        job = scheduler_v1.Job(
            name=existing.name,
            description=description
            or existing.description
            or f"Pushing {dataset_id} data to redivis on daily basis",
            schedule=cron,
            time_zone=self._timezone,
            http_target=self._build_http_target(payload),
            retry_config=self._build_retry_config(),
            attempt_deadline={"seconds": self._attempt_deadline_seconds},
        )
        try:
            self._client.update_job(
                job=job,
                update_mask=field_mask_pb2.FieldMask(
                    paths=[
                        "http_target",
                        "schedule",
                        "time_zone",
                        "description",
                        "retry_config",
                        "attempt_deadline",
                    ]
                ),
            )
            result["updated"] = True
            result["schedule"] = cron
            logging.info(
                "scheduler: migrated job to Run Job API dataset_id=%s name=%s uri=%s",
                dataset_id,
                result["job_name"],
                self._target_url,
            )
        except Exception as e:
            logging.error(
                "scheduler: failed to migrate job dataset_id=%s: %s", dataset_id, e
            )
            result["error"] = f"update_job_error: {e}"
        return result

    def get_or_create_validator_job(
        self,
        *,
        dataset_id: str,
        payload: dict,
        description: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a daily scheduler job if missing. If a job with this id already
        exists, leave it untouched (no payload / schedule update).

        Use ``migrate_all_legacy_to_run_job_api`` for one-time Cloud Function →
        Run Job API target migrations.

        Returns ``{"created", "updated", "already_exists", "job_name", "url", "error"}``.
        """
        result: dict[str, Any] = {
            "created": False,
            "updated": False,
            "already_exists": False,
            "job_name": self.job_name(dataset_id),
            "url": self._target_url,
            "error": None,
        }

        try:
            existing = self._client.get_job(name=result["job_name"])
        except NotFound:
            existing = None

        if existing is not None:
            result["already_exists"] = True
            logging.info(
                "scheduler: job already exists — left unchanged dataset_id=%s name=%s",
                dataset_id,
                result["job_name"],
            )
            return result

        cron = compute_staggered_cron(dataset_id)
        job = scheduler_v1.Job(
            name=result["job_name"],
            description=description
            or f"Pushing {dataset_id} data to redivis on daily basis",
            schedule=cron,
            time_zone=self._timezone,
            http_target=self._build_http_target(payload),
            retry_config=self._build_retry_config(),
            attempt_deadline={"seconds": self._attempt_deadline_seconds},
        )

        try:
            self._client.create_job(parent=self.parent, job=job)
            result["created"] = True
            result["schedule"] = cron
            logging.info(
                "scheduler: created job dataset_id=%s name=%s schedule=%s tz=%s uri=%s",
                dataset_id,
                result["job_name"],
                cron,
                self._timezone,
                self._target_url,
            )
        except AlreadyExists:
            result["already_exists"] = True
            logging.info(
                "scheduler: job already exists (race) dataset_id=%s name=%s",
                dataset_id,
                result["job_name"],
            )
        except Exception as e:
            logging.error(
                "scheduler: failed to create job dataset_id=%s: %s", dataset_id, e
            )
            result["error"] = f"create_job_error: {e}"
        return result
