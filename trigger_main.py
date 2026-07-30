"""
HTTP trigger for the data-validator Cloud Run Job.

Accepts the same clean JSON body as the retired Cloud Function (plus optional
``operation``). Validates ``API-Key``, starts the job, returns HTTP 202.

Local (start Cloud Run Job from laptop):
  ``flask --app trigger_main run --port 8080``

Local (run this checkout's pipeline in-process — use with Postman):
  ``DATA_VALIDATOR_LOCAL_PIPELINE=1 flask --app trigger_main run --port 8080``
  Progress logs print in the Flask terminal; Postman waits for the JSON result
  (raise Postman request timeout — runs can take many minutes).

Production: ``gunicorn --bind :8080 --workers 1 --threads 2 trigger_main:app``
"""

from __future__ import annotations

import json
import logging
import os
import time

from flask import Flask, request
from pydantic import ValidationError

import settings
from shared import utils
from shared.run_job_services import start_validation_job

utils.setup_project_environment()

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

# When true, POST runs the pipeline in this process (local branch code).
# Never set this on the Cloud Run trigger service.
_LOCAL_PIPELINE = os.getenv("DATA_VALIDATOR_LOCAL_PIPELINE", "").strip().lower() in (
    "1",
    "true",
    "yes",
)


def _check_api_key() -> tuple[str, int] | None:
    from shared.secret_services import secret_service

    expected = secret_service.get_secret_payload(
        secret_id=settings.config["VALIDATOR_API_SECRET_ID"],
        version_id="latest",
    ).strip().lower()
    provided = (request.headers.get("API-Key") or "").strip().lower()
    if provided != expected:
        return "Invalid API Key", 403
    return None


def _run_local_pipeline(body: dict) -> tuple[str, int]:
    """Execute the same operations as ``main.py`` in this process."""
    payload = dict(body)
    operation = payload.pop("operation", "data_validation")
    logging.info(
        "LOCAL_PIPELINE=1 — running operation=%s in-process (not Cloud Run)",
        operation,
    )

    if operation != "data_validation":
        # Reuse main.py dispatch for auxiliary ops (exit code only).
        import main as job_main

        runners = {
            "open_assignments_sync": job_main._run_open_assignments_sync,
            "weekly_report": job_main._run_weekly_report,
            "redivis_individual_release": job_main._run_redivis_individual_release,
            "migrate_scheduler_jobs": job_main._run_migrate_scheduler_jobs,
        }
        runner = runners.get(operation)
        if runner is None:
            return json.dumps({"error": "unknown_operation", "operation": operation}), 400
        code = runner(payload)
        return (
            json.dumps(
                {
                    "status": "ok" if code == 0 else "failed",
                    "operation": operation,
                    "exit_code": code,
                    "mode": "local_pipeline",
                    "api_version": settings.config["VERSION"],
                }
            ),
            200 if code == 0 else 500,
        )

    from validators.data_validation_pipeline import run_data_validation

    try:
        dataset_parameters = utils.DatasetParameters(**payload)
    except ValidationError as e:
        return json.dumps({"error": "validation_error", "message": str(e)}), 400

    t0 = time.time()
    logging.info(
        "local data_validation starting dataset_id=%s orgs=%s",
        dataset_parameters.dataset_id,
        len(dataset_parameters.orgs),
    )
    try:
        body_str, status = run_data_validation(
            dataset_parameters,
            start_time=t0,
            # Multi-org: also post short Slack progress; always log to this terminal.
            slack_org_progress=len(dataset_parameters.orgs) > 1,
            slack_summary_always=False,
        )
    except Exception as e:
        logging.exception("local data_validation crashed")
        return (
            json.dumps(
                {
                    "error": "pipeline_crashed",
                    "message": f"{type(e).__name__}: {e}",
                    "mode": "local_pipeline",
                    "api_version": settings.config["VERSION"],
                }
            ),
            500,
        )

    try:
        parsed = json.loads(body_str)
        if isinstance(parsed, dict):
            parsed["mode"] = "local_pipeline"
            body_str = json.dumps(parsed)
    except json.JSONDecodeError:
        pass
    return body_str, status


@app.route("/", methods=["POST"])
def trigger_job():
    auth_err = _check_api_key()
    if auth_err:
        return auth_err

    body = request.get_json(silent=True)
    if not body or not isinstance(body, dict):
        return "Request body must be a JSON object", 400

    if _LOCAL_PIPELINE:
        try:
            body_str, status = _run_local_pipeline(body)
        except Exception as e:
            logging.exception("local pipeline failed")
            return (
                json.dumps(
                    {
                        "error": "local_pipeline_failed",
                        "message": f"{type(e).__name__}: {e}",
                        "api_version": settings.config["VERSION"],
                    }
                ),
                500,
            )
        logging.info("local pipeline finished status=%s", status)
        return body_str, status

    try:
        result = start_validation_job(payload=body)
    except ValueError as e:
        return json.dumps({"error": "validation_error", "message": str(e)}), 400
    except Exception as e:
        logging.exception("start_validation_job failed")
        return json.dumps({
            "error": "job_start_failed",
            "message": f"{type(e).__name__}: {e}",
            "api_version": settings.config["VERSION"],
        }), 500

    response = {
        "status": "accepted",
        "message": (
            "Cloud Run Job started. Monitor Slack for progress and a final summary."
        ),
        "operation": body.get("operation", "data_validation"),
        "batch": result,
        "dataset_id": body.get("dataset_id"),
        "api_version": settings.config["VERSION"],
    }
    logging.info("trigger accepted: %s", json.dumps(response))
    return json.dumps(response), 202


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
