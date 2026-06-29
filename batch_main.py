"""
Cloud Run Job entrypoint: run data validation synchronously (no HTTP timeout).

Expects JSON payload in env ``DATA_VALIDATOR_PAYLOAD`` (same shape as the HTTP API body
without ``operation``).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time

from pydantic import ValidationError

from shared import utils

logging.basicConfig(level=logging.INFO)


def _job_id() -> str:
    execution = (os.environ.get("CLOUD_RUN_EXECUTION") or "").strip()
    if execution:
        return execution.rsplit("/", 1)[-1]
    return f"batch-{int(time.time())}"


def main() -> int:
    utils.setup_project_environment()

    raw = os.environ.get("DATA_VALIDATOR_PAYLOAD")
    if not raw:
        logging.error("DATA_VALIDATOR_PAYLOAD environment variable is not set")
        return 1

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logging.error("Invalid DATA_VALIDATOR_PAYLOAD JSON: %s", e)
        return 1

    try:
        dataset_parameters = utils.DatasetParameters(**data)
    except ValidationError as e:
        logging.error("Payload validation failed: %s", e)
        return 1

    if not dataset_parameters.send_slack:
        dataset_parameters = dataset_parameters.model_copy(update={"send_slack": True})

    job_id = _job_id()
    org_count = len(dataset_parameters.orgs)
    slack_org_progress = org_count > 1

    try:
        from shared.slack_services import format_validation_job_started_slack, notify_slack

        notify_slack(
            message=format_validation_job_started_slack(
                dataset_id=dataset_parameters.dataset_id,
                org_count=org_count,
                job_id=job_id,
            )
        )
    except Exception as e:
        logging.error("batch job start Slack post failed: %s", e)

    from validators.data_validation_pipeline import run_data_validation

    t0 = time.time()
    try:
        body, status = run_data_validation(
            dataset_parameters,
            start_time=t0,
            slack_org_progress=slack_org_progress,
            slack_summary_always=True,
        )
    except Exception as e:
        logging.exception(
            "batch data_validation crashed for dataset_id=%s job_id=%s",
            dataset_parameters.dataset_id,
            job_id,
        )
        try:
            from shared.slack_services import notify_slack

            notify_slack(
                f":rotating_light: *data-validator batch job failed* for "
                f"`{dataset_parameters.dataset_id}` (job `{job_id}`)\n"
                f"```{type(e).__name__}: {e}```"
            )
        except Exception as slack_err:
            logging.error("batch crash-alert Slack post failed: %s", slack_err)
        return 1

    if status != 200:
        logging.error("batch pipeline returned HTTP %s: %s", status, body)
        try:
            from shared.slack_services import notify_slack

            notify_slack(
                f":rotating_light: *data-validator batch job failed* for "
                f"`{dataset_parameters.dataset_id}` (job `{job_id}`)\n"
                f"Pipeline returned status {status}."
            )
        except Exception as slack_err:
            logging.error("batch failure Slack post failed: %s", slack_err)
        return 1

    logging.info("batch job completed successfully for %s", dataset_parameters.dataset_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
