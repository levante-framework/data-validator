"""
Firestore → validate → GCS → Redivis pipeline (``main.py`` / Cloud Run Job calls ``run_data_validation``).
"""

import json
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import settings
from shared import utils
from shared.airtable_services import AirtableServices
from shared.firestore_services import firestore_services
from shared.slack_services import (
    format_data_validation_slack_summary,
    format_org_progress_slack,
    notify_slack,
)
from shared.storage_services import StorageServices
from validators.entity_controller import EntityController
from validators.redivis_services import RedivisServices

logging.basicConfig(level=logging.INFO)


def _today_in_pdt() -> str:
    """YYYY-MM-DD string in America/Los_Angeles for Airtable date columns."""
    return datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d")



def _notify_slack_safe(message: str) -> None:
    try:
        notify_slack(message=message)
    except Exception as e:
        logging.error("Slack notification failed: %s", e)


def run_data_validation(
    dataset_parameters: utils.DatasetParameters,
    *,
    start_time: float | None = None,
    slack_org_progress: bool = False,
    slack_summary_always: bool = False,
) -> tuple[str, int]:
    """
    Run validation and optional GCP / Redivis upload for the given parameters.

    Returns ``(json_body, http_status)`` for logging; the Cloud Run Job entrypoint
    treats non-200 as failure.

    Slack: when ``send_slack`` is true, a summary is posted only if a new Redivis
    version was successfully released (includes ``process_dataset`` status), unless
    ``slack_summary_always`` is set for debugging.
    """
    t0 = start_time if start_time is not None else time.time()

    # Touch the admin client once so the Firestore project is logged before queries.
    firestore_project = firestore_services.admin_db_project_id
    logging.info(
        "Syncing data from Firestore project=%s to Redivis for orgs: %s.",
        firestore_project,
        dataset_parameters.orgs,
    )

    validated_data: dict = {}
    new_version_release = False
    total_validation_stats = {
        "cohorts": 0,
        "administrations": 0,
        "users": {"total": 0, "valid_users": 0},
        "runs": {"total": 0, "valid_runs": 0},
        "trials": {"total": 0, "valid_trials": 0},
        "survey_responses": {"student": 0, "teacher": 0, "caregiver": 0},
        "invalid_data_count": 0,
        "new_schemas": {"runs": [], "trials": [], "surveys": []},
        "orgs": {},
    }
    org_count = len(dataset_parameters.orgs)

    for org_index, org in enumerate(dataset_parameters.orgs, start=1):
        org_t0 = time.time()
        logging.info(f"Getting data from Firestore for org_id: {org.org_id}.")
        if slack_org_progress and dataset_parameters.send_slack:
            _notify_slack_safe(
                format_org_progress_slack(
                    dataset_id=dataset_parameters.dataset_id,
                    org_id=org.org_id,
                    phase="started",
                    index=org_index,
                    total=org_count,
                )
            )
        ec = EntityController(org=org)
        ec.validate_data_from_firestore()
        org_validated_data = ec.get_validated_data()
        if org.is_user_id_masked:
            org_validated_data = utils.pseudonymize_dataset(
                org_validated_data, salt="LEVANTE"
            )
            logging.info("user_ids have been masked.")

        org_validation_stats = {
            "cohorts": len(ec.valid_cohorts) + len(ec.invalid_cohorts),
            "administrations": len(ec.valid_administrations) + len(ec.invalid_administrations),
            "users": {
                "total": len(ec.valid_users) + len(ec.invalid_users),
                "valid_users": sum(1 for user in ec.valid_users if user.valid_user),
            },
            "runs": {
                "total": len(ec.valid_runs) + len(ec.invalid_runs),
                "valid_runs": sum(1 for run in ec.valid_runs if run.valid_run),
            },
            "trials": {
                "total": len(ec.valid_trials) + len(ec.invalid_trials),
                "valid_trials": sum(1 for trial in ec.valid_trials if trial.valid_trial),
            },
            "survey_responses": ec.survey_responses_stats,
            "invalid_data_count": len(org_validated_data.get("invalid_data", [])),
        }
        total_validation_stats["orgs"][org.org_id] = org_validation_stats

        total_validation_stats["cohorts"] += org_validation_stats["cohorts"]
        total_validation_stats["administrations"] += org_validation_stats["administrations"]
        total_validation_stats["users"]["total"] += org_validation_stats["users"]["total"]
        total_validation_stats["users"]["valid_users"] += org_validation_stats["users"]["valid_users"]
        total_validation_stats["runs"]["total"] += org_validation_stats["runs"]["total"]
        total_validation_stats["runs"]["valid_runs"] += org_validation_stats["runs"]["valid_runs"]
        total_validation_stats["trials"]["total"] += org_validation_stats["trials"]["total"]
        total_validation_stats["trials"]["valid_trials"] += org_validation_stats["trials"]["valid_trials"]
        total_validation_stats["survey_responses"]["student"] += org_validation_stats["survey_responses"]["student"]
        total_validation_stats["survey_responses"]["teacher"] += org_validation_stats["survey_responses"]["teacher"]
        total_validation_stats["survey_responses"]["caregiver"] += org_validation_stats["survey_responses"]["caregiver"]
        total_validation_stats["invalid_data_count"] += org_validation_stats["invalid_data_count"]
        total_validation_stats["new_schemas"]["runs"].extend(ec.new_schemas["runs"])
        total_validation_stats["new_schemas"]["trials"].extend(ec.new_schemas["trials"])
        total_validation_stats["new_schemas"]["surveys"].extend(ec.new_schemas["surveys"])
        validated_data = utils.merge_dictionaries(validated_data, org_validated_data)
        if slack_org_progress and dataset_parameters.send_slack:
            _notify_slack_safe(
                format_org_progress_slack(
                    dataset_id=dataset_parameters.dataset_id,
                    org_id=org.org_id,
                    phase="finished",
                    index=org_index,
                    total=org_count,
                    elapsed_seconds=time.time() - org_t0,
                    stats=org_validation_stats,
                )
            )

    reduce_dup_keys = {
        "sites": "site_id",
        "cohorts": "cohort_id",
        "schools": "school_id",
        "classes": "class_id",
        "administrations": "administration_id",
        "tasks": "task_id",
        "variants": "variant_id",
        "users": "user_id",
        "runs": "run_id",
        "trials": "trial_id",
    }

    validated_data = utils.reduce_duplication_by_keys(data=validated_data, keys=reduce_dup_keys)
    validated_data = utils.append_schema_rows_to_validated_data(validated_data)

    if not dataset_parameters.is_save_to_storage:
        elapsed_time = time.time() - t0
        output = {
            "title": "Function executed successfully! Nothing uploaded to GCP or Redivis.",
            "elapsed_time": elapsed_time,
            "is_save_to_storage": dataset_parameters.is_save_to_storage,
            "is_force_uploading_to_redivis": dataset_parameters.is_force_uploading_to_redivis,
            "total_validation_stats": total_validation_stats,
        }
        logging.info(json.dumps(output, cls=utils.CustomJSONEncoder))

        # Validation-only / no-upload runs stay quiet unless explicitly forced.
        if dataset_parameters.send_slack and slack_summary_always:
            slack_response = {
                "dataset_parameters": dataset_parameters.to_dict(),
                "logs": {"total_validation_stats": total_validation_stats},
                "elapsed_time": elapsed_time,
                "api_version": settings.config["VERSION"],
                "new_version_release": False,
            }
            _notify_slack_safe(message=format_data_validation_slack_summary(slack_response))

        return json.dumps(output, cls=utils.CustomJSONEncoder), 200

    if slack_org_progress and dataset_parameters.send_slack:
        _notify_slack_safe(
            f":package: *All {org_count} site(s) validated* for `{dataset_parameters.dataset_id}` "
            f"— merging, deduplicating, and uploading…"
        )

    logging.info(f"Saving data to GCP storage for dataset_id: {dataset_parameters.dataset_id}.")

    storage = StorageServices(
        cred=firestore_services.admin_credentials,
        dataset_id=dataset_parameters.dataset_id,
        is_forced_uploading_redivis=dataset_parameters.is_force_uploading_to_redivis,
    )
    storage.process(validated_data=validated_data)

    process_log = None
    # Redivis ingests each table from its GCS blob, so releasing after a failed write
    # would publish stale tables and hand process_dataset empty input.
    if storage.is_new_version_needed and storage.has_upload_failures:
        logging.error(
            "Skipping Redivis upload/release for %s: %s GCS table upload(s) failed.",
            dataset_parameters.dataset_id,
            len(storage.upload_to_GCP_log["file_uploads_fail"]),
        )

    if storage.is_new_version_needed and not storage.has_upload_failures:
        logging.info(f"Uploading data to Redivis for dataset_id: {dataset_parameters.dataset_id}.")

        rs = RedivisServices()

        rs.set_dataset(dataset_id=dataset_parameters.dataset_id)
        rs.create_dateset_version(params=dataset_parameters.to_dict()["orgs"])
        if rs.upload_to_redivis_log["dataset_fails"]:
            logging.info("Process stops at create_dateset_version.")
        else:
            file_names = storage.list_blobs_with_prefix()
            logging.info(f"GCP bucket {dataset_parameters.dataset_id} has files {file_names}.")

            for file_name in file_names:
                rs.save_to_redivis_table(file_name=file_name)

            table_names_in_redivis = [table.name for table in rs.dataset.list_tables()]
            table_names_in_gcp_bucket = [name.split("/")[-1].split(".")[0] for name in file_names]

            exception_tables = ["invalid_data"]
            for table_name in exception_tables:
                if table_name in table_names_in_redivis and table_name not in table_names_in_gcp_bucket:
                    rs.delete_table(table_name=table_name)

            fails_before_release = len(rs.upload_to_redivis_log["dataset_fails"])
            rs.release_dataset(params=dataset_parameters.to_dict())
            release_ok = len(rs.upload_to_redivis_log["dataset_fails"]) == fails_before_release
            if release_ok:
                new_version_release = True

            # After a successful raw release, run process_dataset for this site:
            # source = dataset_id (*-raw), target = unmarked {Name}.
            # Opt out with skip_process_dataset=true (cron payloads omit it).
            raw_suffix = settings.config["RAW_DATASET_SUFFIX"]
            raw_id = dataset_parameters.dataset_id or ""
            if release_ok and raw_id.endswith(raw_suffix):
                if dataset_parameters.skip_process_dataset:
                    processed_id = RedivisServices.processed_name_from_raw(raw_id)
                    process_log = {
                        "ran": False,
                        "skipped": True,
                        "raw_dataset_id": raw_id,
                        "processed_dataset_id": processed_id,
                        "workflow": settings.config["REDIVIS_PROCESS_WORKFLOW_NAME"],
                        "notebook": settings.config["REDIVIS_PROCESS_NOTEBOOK_NAME"],
                        "error": None,
                        "airtable": None,
                    }
                    logging.info(
                        "skip_process_dataset=true — skipped process_dataset for %r",
                        raw_id,
                    )
                else:
                    process_log = rs.run_process_dataset_workflow(
                        raw_dataset_id=raw_id,
                        release_processed=dataset_parameters.release_processed_dataset,
                    )
                    rel = process_log.get("processed_release") or {}
                    stamp_airtable = rel.get("released") or (
                        rel.get("skipped")
                        and rel.get("skip_reason") != "release_processed_dataset=false"
                    )
                    if (
                        process_log.get("ran")
                        and not process_log.get("error")
                        and stamp_airtable
                    ):
                        try:
                            airtable = AirtableServices()
                            airtable_log = airtable.update_processed_dataset_last_update(
                                processed_name=process_log["processed_dataset_id"],
                                date_yyyy_mm_dd=_today_in_pdt(),
                            )
                            process_log["airtable"] = airtable_log
                        except Exception as e:
                            logging.error(
                                "Airtable processed-date update failed for %r: %s",
                                process_log.get("processed_dataset_id"),
                                e,
                            )
                            process_log["airtable"] = {
                                "updated": False,
                                "record_id": None,
                                "error": str(e),
                            }
                    else:
                        process_log.setdefault("airtable", None)

        rs.upload_to_redivis_log["table_counts"] = rs.count_tables()
        output = {
            "total_validation_stats": total_validation_stats,
            "gcp_logs": storage.upload_to_GCP_log,
            "redivis_logs": rs.upload_to_redivis_log,
        }
        if process_log is not None:
            output["process_dataset"] = process_log
    else:
        output = {
            "total_validation_stats": total_validation_stats,
            "gcp_logs": storage.upload_to_GCP_log,
        }

    elapsed_time = time.time() - t0
    response = {
        "operation": "data_validation",
        "dataset_parameters": dataset_parameters.to_dict(),
        "firestore_project": firestore_project,
        "logs": output,
        "elapsed_time": elapsed_time,
        "api_version": settings.config["VERSION"],
        "new_version_release": new_version_release,
        "process_dataset": (output or {}).get("process_dataset"),
    }
    logging.info(json.dumps(response))
    firestore_services.set_logs_to_firebase(response=response, dataset_id=dataset_parameters.dataset_id)

    # Live per-site runs: Slack only when a new Redivis version was released
    # (summary includes process_dataset when that workflow ran), or when GCS writes
    # failed — those runs never reach a release and would otherwise be silent.
    if dataset_parameters.send_slack and (
        slack_summary_always or new_version_release or storage.has_upload_failures
    ):
        _notify_slack_safe(message=format_data_validation_slack_summary(response))

    return json.dumps(response), 200
