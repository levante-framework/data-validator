import json
import logging
import time

from flask import Flask, request
from pydantic import ValidationError

import settings
from shared import utils

utils.setup_project_environment()

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)


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

    # Default: full data validation + optional GCP / Redivis pipeline
    try:
        dataset_parameters = utils.DatasetParameters(**data)
    except ValidationError as e:
        return json.dumps({"error": "validation_error", "detail": e.errors()}), 400
    except Exception as e:
        return str(e), 400

    from validators.data_validation_pipeline import run_data_validation

    try:
        return run_data_validation(dataset_parameters, start_time=start_time)
    except Exception as e:
        logging.exception(
            "data_validation crashed for dataset_id=%s",
            dataset_parameters.dataset_id,
        )
        # Try to alert Slack so silent crashes don't go unnoticed; never let a
        # Slack failure mask the original error.
        try:
            from shared.slack_services import notify_slack

            notify_slack(
                f":rotating_light: *data-validator crashed* for "
                f"`{dataset_parameters.dataset_id}`\n"
                f"```{type(e).__name__}: {e}```"
            )
        except Exception as slack_err:
            logging.error(
                "data_validation crash-alert Slack post failed: %s", slack_err
            )
        return json.dumps({
            "error": "pipeline_crashed",
            "dataset_id": dataset_parameters.dataset_id,
            "message": f"{type(e).__name__}: {e}",
            "elapsed_time": time.time() - start_time,
            "api_version": settings.config["VERSION"],
        }), 500


def data_validator(request):
    return process(request)


@app.route("/", methods=["POST"])
def local_run():
    return process(request)


if __name__ == "__main__":
    app.run(port=8080)
