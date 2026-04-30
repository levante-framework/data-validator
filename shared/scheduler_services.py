"""Cloud Scheduler helpers used to provision the per-site daily data-validator jobs."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from typing import Any

from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud import scheduler_v1

import settings
from shared.secret_services import secret_service

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
        self._target_url = settings.config["DATA_VALIDATOR_FUNCTION_URL_TEMPLATE"].format(
            project_id=self._project_id
        )
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

    def _build_retry_config(self) -> "scheduler_v1.RetryConfig":
        cfg = self._retry_config_dict
        return scheduler_v1.RetryConfig(
            retry_count=cfg["retry_count"],
            max_retry_duration={"seconds": cfg["max_retry_duration_seconds"]},
            min_backoff_duration={"seconds": cfg["min_backoff_seconds"]},
            max_backoff_duration={"seconds": cfg["max_backoff_seconds"]},
            max_doublings=cfg["max_doublings"],
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

    def get_or_create_validator_job(
        self,
        *,
        dataset_id: str,
        payload: dict,
        description: str | None = None,
    ) -> dict[str, Any]:
        """
        Idempotent: if a job for this dataset_id already exists, no changes are made.
        Returns ``{"created", "already_exists", "job_name", "url", "error"}``.
        """
        result: dict[str, Any] = {
            "created": False,
            "already_exists": False,
            "job_name": self.job_name(dataset_id),
            "url": self._target_url,
            "error": None,
        }
        if self.job_exists(dataset_id):
            result["already_exists"] = True
            logging.info(
                "scheduler: job already exists for dataset_id=%s name=%s",
                dataset_id,
                result["job_name"],
            )
            return result

        try:
            api_key = secret_service.get_secret_payload(
                secret_id=settings.config["VALIDATOR_API_SECRET_ID"]
            ).strip()
        except Exception as e:
            logging.error("scheduler: failed to read API key secret: %s", e)
            result["error"] = f"api_key_secret_error: {e}"
            return result

        body = json.dumps(payload).encode("utf-8")
        cron = compute_staggered_cron(dataset_id)
        job = scheduler_v1.Job(
            name=result["job_name"],
            description=description
            or f"Pushing {dataset_id} data to redivis on daily basis",
            schedule=cron,
            time_zone=self._timezone,
            http_target=scheduler_v1.HttpTarget(
                uri=self._target_url,
                http_method=scheduler_v1.HttpMethod.POST,
                headers={
                    "Content-Type": "application/json",
                    "API-Key": api_key,
                },
                body=body,
            ),
            retry_config=self._build_retry_config(),
            attempt_deadline={"seconds": self._attempt_deadline_seconds},
        )

        try:
            self._client.create_job(parent=self.parent, job=job)
            result["created"] = True
            result["schedule"] = cron
            logging.info(
                "scheduler: created job dataset_id=%s name=%s schedule=%s tz=%s",
                dataset_id,
                result["job_name"],
                cron,
                self._timezone,
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
