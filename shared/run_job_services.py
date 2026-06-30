"""Helpers for starting the data-validator Cloud Run Job programmatically."""

from __future__ import annotations

import json
import logging
import os

import settings

logging.basicConfig(level=logging.INFO)

_MAX_PAYLOAD_BYTES = 30_000


def _project_id() -> str:
    pid = os.environ.get("project_id") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not pid:
        raise RuntimeError("GCP project id is not set in the environment.")
    return pid


def run_job_api_uri(*, project_id: str | None = None, region: str | None = None) -> str:
    """Cloud Run Admin API URI used by Cloud Scheduler HTTP targets."""
    pid = project_id or _project_id()
    reg = region or settings.config["CLOUD_RUN_JOB_REGION"]
    job_id = settings.config["CLOUD_RUN_JOB_NAME"]
    return (
        f"https://run.googleapis.com/v2/projects/{pid}/locations/{reg}/jobs/{job_id}:run"
    )


def build_run_job_request_body(*, payload: dict) -> bytes:
    """
    Serialize the Run Job API request body with ``DATA_VALIDATOR_PAYLOAD`` injected.

    ``payload`` is the job JSON (may include ``operation``).
    """
    payload_json = json.dumps(payload, separators=(",", ":"))
    if len(payload_json.encode("utf-8")) > _MAX_PAYLOAD_BYTES:
        raise ValueError(
            f"Payload is {len(payload_json.encode('utf-8'))} bytes; "
            f"max {_MAX_PAYLOAD_BYTES} for Cloud Run env injection."
        )
    body = {
        "overrides": {
            "containerOverrides": [
                {"env": [{"name": "DATA_VALIDATOR_PAYLOAD", "value": payload_json}]}
            ]
        }
    }
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


def start_validation_job(*, payload: dict) -> dict:
    """
    Execute the data-validator Cloud Run Job with ``DATA_VALIDATOR_PAYLOAD``.

    Returns metadata about the started execution.
    """
    from google.cloud import run_v2
    from google.cloud.run_v2.types import EnvVar, RunJobRequest

    project_id = _project_id()
    region = settings.config["CLOUD_RUN_JOB_REGION"]
    job_id = settings.config["CLOUD_RUN_JOB_NAME"]
    job_name = f"projects/{project_id}/locations/{region}/jobs/{job_id}"

    payload_json = json.dumps(payload, separators=(",", ":"))
    if len(payload_json.encode("utf-8")) > _MAX_PAYLOAD_BYTES:
        raise ValueError(
            f"Payload is {len(payload_json.encode('utf-8'))} bytes; "
            f"max {_MAX_PAYLOAD_BYTES} for Cloud Run env injection."
        )

    request = RunJobRequest(
        name=job_name,
        overrides=RunJobRequest.Overrides(
            container_overrides=[
                RunJobRequest.Overrides.ContainerOverride(
                    env=[EnvVar(name="DATA_VALIDATOR_PAYLOAD", value=payload_json)]
                )
            ]
        ),
    )

    client = run_v2.JobsClient()
    operation = client.run_job(request=request)

    operation_name = getattr(operation, "name", None) or str(operation)
    logging.info(
        "run_job: started job=%s operation=%s dataset_id=%s",
        job_name,
        operation_name,
        payload.get("dataset_id"),
    )
    return {
        "job_name": job_id,
        "job_resource": job_name,
        "operation_name": operation_name,
        "dataset_id": payload.get("dataset_id"),
        "task_timeout_seconds": settings.config["CLOUD_RUN_JOB_TASK_TIMEOUT_SECONDS"],
    }
