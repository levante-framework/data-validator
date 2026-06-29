"""Start Cloud Run batch validation jobs (long-running all-sites exports)."""

from __future__ import annotations

import json
import logging
import os

import settings

logging.basicConfig(level=logging.INFO)

# Cloud Run env var total size is limited; keep payloads under ~30 KiB.
_MAX_PAYLOAD_BYTES = 30_000


def _project_id() -> str:
    pid = os.environ.get("project_id") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not pid:
        raise RuntimeError("GCP project id is not set in the environment.")
    return pid


def start_batch_validation_job(*, payload: dict) -> dict:
    """
    Execute ``data-validator-batch`` with ``DATA_VALIDATOR_PAYLOAD`` set to ``payload``.

    Returns metadata about the started execution (LRO name when available).
    """
    from google.cloud import run_v2
    from google.cloud.run_v2.types import EnvVar, RunJobRequest

    project_id = _project_id()
    region = settings.config["CLOUD_RUN_BATCH_JOB_REGION"]
    job_id = settings.config["CLOUD_RUN_BATCH_JOB_NAME"]
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
        "batch_job: started job=%s operation=%s dataset_id=%s",
        job_name,
        operation_name,
        payload.get("dataset_id"),
    )
    return {
        "job_name": job_id,
        "job_resource": job_name,
        "operation_name": operation_name,
        "dataset_id": payload.get("dataset_id"),
        "task_timeout_seconds": settings.config["CLOUD_RUN_BATCH_TASK_TIMEOUT_SECONDS"],
    }
