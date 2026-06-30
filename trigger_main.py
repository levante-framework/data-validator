"""
HTTP trigger for the data-validator Cloud Run Job.

Accepts the same clean JSON body as the retired Cloud Function (plus optional
``operation``). Validates ``API-Key``, starts the job, returns HTTP 202.

Local: ``flask --app trigger_main run --port 8080``
Production: ``gunicorn --bind :8080 --workers 1 --threads 2 trigger_main:app``
"""

from __future__ import annotations

import json
import logging

from flask import Flask, request

import settings
from shared import utils
from shared.run_job_services import start_validation_job

utils.setup_project_environment()

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)


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


@app.route("/", methods=["POST"])
def trigger_job():
    auth_err = _check_api_key()
    if auth_err:
        return auth_err

    body = request.get_json(silent=True)
    if not body or not isinstance(body, dict):
        return "Request body must be a JSON object", 400

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
