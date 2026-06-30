"""
Cloud Run Job entrypoint for data-validator.

Set ``DATA_VALIDATOR_PAYLOAD`` to a JSON object, or pass a ``.json`` file path as
the first CLI argument, or set ``DATA_VALIDATOR_PAYLOAD_FILE``, or pipe JSON on stdin.

- ``data_validation`` (default) — full Firestore → validate → GCS → Redivis pipeline
- ``open_assignments_sync`` — sync open assignments from Airtable
- ``weekly_report`` — weekly ops report to Slack
- ``redivis_individual_release`` — Airtable ↔ Redivis individual provisioning
- ``migrate_scheduler_jobs`` — migrate daily cron jobs to the Cloud Run Job API

For ``data_validation``, the payload matches ``DatasetParameters`` (same as before,
without wrapping in ``operation``).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

from pydantic import ValidationError

from shared import utils
from shared.payload_loader import load_payload_dict

logging.basicConfig(level=logging.INFO)


def _job_id() -> str:
    execution = (os.environ.get("CLOUD_RUN_EXECUTION") or "").strip()
    if execution:
        return execution.rsplit("/", 1)[-1]
    return f"local-{uuid.uuid4().hex[:8]}"


def _run_data_validation(data: dict) -> int:
    from validators.data_validation_pipeline import run_data_validation

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
        logging.error("job start Slack post failed: %s", e)

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
            "data_validation crashed for dataset_id=%s job_id=%s",
            dataset_parameters.dataset_id,
            job_id,
        )
        try:
            from shared.slack_services import notify_slack

            notify_slack(
                f":rotating_light: *data-validator job failed* for "
                f"`{dataset_parameters.dataset_id}` (job `{job_id}`)\n"
                f"```{type(e).__name__}: {e}```"
            )
        except Exception as slack_err:
            logging.error("crash-alert Slack post failed: %s", slack_err)
        return 1

    if status != 200:
        logging.error("pipeline returned HTTP %s: %s", status, body)
        try:
            from shared.slack_services import notify_slack

            notify_slack(
                f":rotating_light: *data-validator job failed* for "
                f"`{dataset_parameters.dataset_id}` (job `{job_id}`)\n"
                f"Pipeline returned status {status}."
            )
        except Exception as slack_err:
            logging.error("failure Slack post failed: %s", slack_err)
        return 1

    logging.info("job completed successfully for %s", dataset_parameters.dataset_id)
    return 0


def _run_open_assignments_sync(data: dict) -> int:
    from sync.open_assignments import sync_open_assignments_from_airtable

    dry_run = bool(data.get("dry_run", False))
    try:
        result = sync_open_assignments_from_airtable(dry_run=dry_run)
    except ValueError as e:
        logging.error("open_assignments_sync failed: %s", e)
        return 1
    logging.info("open_assignments_sync result: %s", json.dumps(result, cls=utils.CustomJSONEncoder))
    return 0


def _run_weekly_report(data: dict) -> int:
    from validators.weekly_report import run_weekly_report

    dry_run = bool(data.get("dry_run", False))
    try:
        result = run_weekly_report(dry_run=dry_run)
    except Exception:
        logging.exception("weekly_report crashed")
        return 1
    logging.info("weekly_report result keys: %s", sorted(result.keys()))
    return 0


def _run_migrate_scheduler_jobs(data: dict) -> int:
    from shared.scheduler_services import SchedulerServices

    dry_run = bool(data.get("dry_run", False))
    if dry_run:
        logging.info("migrate_scheduler_jobs dry_run=true — listing jobs only")
        try:
            from google.cloud import scheduler_v1

            scheduler = SchedulerServices()
            legacy = []
            for job in scheduler._client.list_jobs(parent=scheduler.parent):
                if scheduler._needs_target_migration(job):
                    legacy.append(job.name.rsplit("/", 1)[-1])
            logging.info(
                "migrate_scheduler_jobs dry_run: %s job(s) would migrate: %s",
                len(legacy),
                legacy,
            )
        except Exception:
            logging.exception("migrate_scheduler_jobs dry_run failed")
            return 1
        return 0

    try:
        scheduler = SchedulerServices()
        summary = scheduler.migrate_all_legacy_scheduler_jobs()
    except Exception:
        logging.exception("migrate_scheduler_jobs failed")
        return 1

    if summary.get("errors"):
        logging.error("migrate_scheduler_jobs completed with errors: %s", summary)
        return 1
    logging.info("migrate_scheduler_jobs completed: %s", summary)
    return 0


def _run_redivis_individual_release(data: dict) -> int:
    from sync.redivis_release import check_redivis_individual_release_awaiting_slack

    dry_run = bool(data.get("dry_run", False))
    dataset_name_raw = data.get("dataset_name")
    dataset_name = (
        str(dataset_name_raw).strip() if dataset_name_raw is not None else None
    ) or None
    try:
        check_redivis_individual_release_awaiting_slack(
            dry_run=dry_run, dataset_name=dataset_name
        )
    except ValueError as e:
        logging.error("redivis_individual_release failed: %s", e)
        return 1
    return 0


def main() -> int:
    utils.setup_project_environment()

    try:
        payload = load_payload_dict()
    except RuntimeError as e:
        logging.error("%s", e)
        return 1

    operation = payload.pop("operation", "data_validation")
    logging.info(
        "data-validator job starting operation=%s at %s",
        operation,
        datetime.now(timezone.utc).isoformat(),
    )

    if operation == "data_validation":
        return _run_data_validation(payload)
    if operation == "open_assignments_sync":
        return _run_open_assignments_sync(payload)
    if operation == "weekly_report":
        return _run_weekly_report(payload)
    if operation == "redivis_individual_release":
        return _run_redivis_individual_release(payload)
    if operation == "migrate_scheduler_jobs":
        return _run_migrate_scheduler_jobs(payload)

    logging.error("Unknown operation: %r", operation)
    return 1


if __name__ == "__main__":
    sys.exit(main())
