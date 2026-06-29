import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone

from flask import Flask, request
from pydantic import ValidationError

import settings
from shared import utils

utils.setup_project_environment()

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)


def _run_data_validation_background(
    dataset_parameters: utils.DatasetParameters,
    *,
    start_time: float,
    job_id: str,
    slack_org_progress: bool,
) -> None:
    from validators.data_validation_pipeline import run_data_validation

    try:
        run_data_validation(
            dataset_parameters,
            start_time=start_time,
            slack_org_progress=slack_org_progress,
            slack_summary_always=True,
        )
    except Exception as e:
        logging.exception(
            "data_validation crashed for dataset_id=%s job_id=%s",
            dataset_parameters.dataset_id,
            job_id,
        )
        try:
            from shared.slack_services import notify_slack

            notify_slack(
                f":rotating_light: *data-validator failed* for "
                f"`{dataset_parameters.dataset_id}` (job `{job_id}`)\n"
                f"```{type(e).__name__}: {e}```"
            )
        except Exception as slack_err:
            logging.error("crash-alert Slack post failed: %s", slack_err)


def _accept_data_validation_job(
    dataset_parameters: utils.DatasetParameters,
    *,
    start_time: float,
) -> tuple[str, int]:
    """Return 202 immediately; run validation in a background thread."""
    job_id = (
        f"{dataset_parameters.dataset_id}-"
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-"
        f"{uuid.uuid4().hex[:8]}"
    )
    org_count = len(dataset_parameters.orgs)
    slack_org_progress = org_count > 1 and dataset_parameters.send_slack

    if dataset_parameters.send_slack:
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
            logging.error("job start Slack post failed: %s", e)

    thread = threading.Thread(
        target=_run_data_validation_background,
        kwargs={
            "dataset_parameters": dataset_parameters,
            "start_time": start_time,
            "job_id": job_id,
            "slack_org_progress": slack_org_progress,
        },
        daemon=False,
    )
    thread.start()
    response = {
        "operation": "data_validation",
        "status": "accepted",
        "job_id": job_id,
        "dataset_id": dataset_parameters.dataset_id,
        "org_count": org_count,
        "slack_org_progress": slack_org_progress,
        "message": (
            "Validation started in the background. "
            "Monitor Slack for progress and a final summary."
        ),
        "api_version": settings.config["VERSION"],
    }
    logging.info(json.dumps(response))
    return json.dumps(response), 202


def process(req):
    start_time = time.time()
    from shared.secret_services import secret_service

    admin_api_key = secret_service.get_secret_payload(
        secret_id=settings.config["VALIDATOR_API_SECRET_ID"],
        version_id="latest",
    ).strip().lower()

    api_key = req.headers.get("API-Key")
    api_key = api_key.strip().lower()

    if api_key != admin_api_key:
        return "Invalid API Key", 403

    if req.method != "POST":
        return "Function needs to receive POST request", 500

    request_json = req.get_json(silent=True)
    if not request_json:
        return "Request body is not received properly", 500

    data = dict(request_json)
    operation = data.pop("operation", "data_validation")

    if operation == "open_assignments_sync":
        from sync.open_assignments import sync_open_assignments_from_airtable

        dry_run = bool(data.pop("dry_run", False))
        try:
            result = sync_open_assignments_from_airtable(dry_run=dry_run)
        except ValueError as e:
            return str(e), 400
        elapsed_time = time.time() - start_time
        response = {
            "operation": operation,
            "result": result,
            "elapsed_time": elapsed_time,
            "api_version": settings.config["VERSION"],
        }
        logging.info(json.dumps(response, cls=utils.CustomJSONEncoder))
        return json.dumps(response, cls=utils.CustomJSONEncoder), 200

    if operation == "weekly_report":
        from validators.weekly_report import run_weekly_report

        dry_run = bool(data.pop("dry_run", False))
        try:
            result = run_weekly_report(dry_run=dry_run)
        except Exception as e:
            logging.exception("weekly_report crashed")
            return json.dumps({"error": "weekly_report_crashed",
                               "message": f"{type(e).__name__}: {e}"}), 500
        elapsed_time = time.time() - start_time
        response = {
            "operation": operation,
            "result": result,
            "elapsed_time": elapsed_time,
            "api_version": settings.config["VERSION"],
        }
        logging.info(json.dumps(response, cls=utils.CustomJSONEncoder))
        return json.dumps(response, cls=utils.CustomJSONEncoder), 200

    if operation == "redivis_individual_release":
        from sync.redivis_release import check_redivis_individual_release_awaiting_slack

        dry_run = bool(data.pop("dry_run", False))
        dataset_name_raw = data.pop("dataset_name", None)
        dataset_name = (
            str(dataset_name_raw).strip() if dataset_name_raw is not None else None
        ) or None
        try:
            result = check_redivis_individual_release_awaiting_slack(
                dry_run=dry_run, dataset_name=dataset_name
            )
        except ValueError as e:
            return str(e), 400
        elapsed_time = time.time() - start_time
        response = {
            "operation": operation,
            "result": result,
            "elapsed_time": elapsed_time,
            "api_version": settings.config["VERSION"],
        }
        logging.info(json.dumps(response, cls=utils.CustomJSONEncoder))
        return json.dumps(response, cls=utils.CustomJSONEncoder), 200

    if operation == "start_batch_job":
        try:
            dataset_parameters = utils.DatasetParameters(**data)
        except ValidationError as e:
            return json.dumps({"error": "validation_error", "detail": e.errors()}), 400
        except Exception as e:
            return str(e), 400

        from shared.batch_job_services import start_batch_validation_job

        try:
            batch_result = start_batch_validation_job(payload=data)
        except Exception as e:
            logging.exception("start_batch_job failed for dataset_id=%s", data.get("dataset_id"))
            return json.dumps({
                "error": "batch_job_start_failed",
                "dataset_id": data.get("dataset_id"),
                "message": f"{type(e).__name__}: {e}",
                "api_version": settings.config["VERSION"],
            }), 500

        response = {
            "operation": operation,
            "status": "accepted",
            "message": (
                "Cloud Run batch job started. Monitor Slack for progress and a final summary. "
                f"Task timeout: {settings.config['CLOUD_RUN_BATCH_TASK_TIMEOUT_SECONDS']}s."
            ),
            "batch": batch_result,
            "dataset_id": dataset_parameters.dataset_id,
            "org_count": len(dataset_parameters.orgs),
            "api_version": settings.config["VERSION"],
        }
        logging.info(json.dumps(response))
        return json.dumps(response), 202

    # Default: full data validation + optional GCP / Redivis pipeline (always fire-and-forget).
    try:
        dataset_parameters = utils.DatasetParameters(**data)
    except ValidationError as e:
        return json.dumps({"error": "validation_error", "detail": e.errors()}), 400
    except Exception as e:
        return str(e), 400

    return _accept_data_validation_job(dataset_parameters, start_time=start_time)


def data_validator(request):
    return process(request)


@app.route("/", methods=["POST"])
def local_run():
    return process(request)


if __name__ == "__main__":
    app.run(port=8080)
