#!/usr/bin/env python3
"""
Force-refresh every site that already has a processed Redivis release.

Reads the LEVANTE Entities Dataset table and refreshes each row whose
``Redivis processed dataset last update`` is set. Each run writes GCS,
force-uploads a new raw version, runs process_dataset, releases the processed
dataset, and posts the finishing Slack log.

``--runtime cloud`` (default) starts one Cloud Run Job per site and returns.
``--runtime local`` runs the same pipeline in this process, one site at a time.

No site is excluded. Rows with an empty processed-date column are skipped, as
are rows that have no Firestore siteId (the job would have nothing to query).

Examples:
  python force_data_refresh.py --dry-run
  python force_data_refresh.py
  python force_data_refresh.py --runtime local
"""

from __future__ import annotations

import argparse
import json
import sys

import settings
from shared import utils
from shared.airtable_services import AirtableServices
from shared.run_job_services import start_validation_job

utils.setup_project_environment()


def _date_is_empty(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _not_empty_formula() -> str:
    field = settings.config["AIRTABLE_FIELD_PROCESSED_DATASET_LAST_UPDATE"]
    return f"NOT({{{field}}}='')"


def list_refresh_sites() -> tuple[list[dict], list[dict]]:
    """
    Return ``(ready, skipped)`` Dataset rows.

    ``ready`` rows have a processed last-update date, a raw dataset name, and
    a real Firestore siteId. ``skipped`` rows matched the date filter but
    cannot be launched.
    """
    name_field = settings.config["AIRTABLE_FIELD_REDIVIS_DATASET_NAME"]
    raw_name_field = settings.config["AIRTABLE_FIELD_REDIVIS_NAME"]
    site_field = settings.config["AIRTABLE_FIELD_FIRESTORE_SITE_ID"]
    last_field = settings.config["AIRTABLE_FIELD_PROCESSED_DATASET_LAST_UPDATE"]
    raw_suffix = settings.config.get("RAW_DATASET_SUFFIX", "-raw")
    placeholder = settings.config.get("MISSING_SITE_ID_PLACEHOLDER", "missing_site_id")

    rows = AirtableServices().list_dataset_records(formula=_not_empty_formula())
    ready: list[dict] = []
    skipped: list[dict] = []
    for row in rows:
        fields = row.get("fields") or {}
        last_update = fields.get(last_field)
        if _date_is_empty(last_update):
            continue
        name = (fields.get(name_field) or "").strip()
        raw_name = (fields.get(raw_name_field) or "").strip() or (
            f"{name}{raw_suffix}" if name else ""
        )
        site_id = (fields.get(site_field) or "").strip()
        item = {
            "airtable_record_id": row.get("id"),
            "dataset_name": name,
            "dataset_id": raw_name,
            "site_id": site_id,
            "processed_last_update": last_update,
        }
        if not raw_name:
            item["reason"] = "missing Redivis name and Name"
            skipped.append(item)
            continue
        if not site_id or site_id == placeholder:
            item["reason"] = "missing Firestore siteId"
            skipped.append(item)
            continue
        ready.append(item)
    ready.sort(key=lambda item: item["dataset_id"])
    return ready, skipped


def build_refresh_payload(*, dataset_id: str, site_id: str) -> dict:
    """Force a new raw version and run through processed-dataset release."""
    payload = {
        "dataset_id": dataset_id,
        "is_save_to_storage": True,
        "is_force_uploading_to_redivis": True,
        "skip_process_dataset": False,
        "release_processed_dataset": True,
        "send_slack": True,
        "orgs": [
            {
                "org_id": dataset_id,
                "is_guest": False,
                "filters": {
                    "org_filter": {
                        "key": "districts",
                        "operator": "array_contains_any",
                        "value": [site_id],
                    }
                },
            }
        ],
    }
    utils.DatasetParameters.model_validate(payload)
    return payload


def _run_local(payload: dict) -> int:
    """Same entry as ``python main.py`` for one dataset payload."""
    from main import _run_data_validation

    return _run_data_validation(dict(payload))


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Force-refresh every Airtable site with a processed dataset last update"
        )
    )
    parser.add_argument(
        "--runtime",
        choices=("cloud", "local"),
        default="cloud",
        help=(
            "cloud starts a Cloud Run Job per site (default). "
            "local runs the pipeline in this process, one site at a time."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the sites and payloads without starting jobs",
    )
    args = parser.parse_args()

    ready, skipped = list_refresh_sites()
    print(
        f"runtime={args.runtime} sites with processed last update: "
        f"{len(ready)} ready, {len(skipped)} skipped"
    )
    for item in skipped:
        print(
            f"SKIP {item.get('dataset_id') or item.get('dataset_name') or item['airtable_record_id']}"
            f" — {item['reason']}"
        )

    started = []
    failed = []
    for item in ready:
        payload = build_refresh_payload(
            dataset_id=item["dataset_id"], site_id=item["site_id"]
        )
        if args.dry_run:
            print(json.dumps({"runtime": args.runtime, "site": item, "payload": payload}, indent=2))
            continue
        if args.runtime == "local":
            print(f"RUNNING local {item['dataset_id']}", flush=True)
            try:
                code = _run_local(payload)
            except Exception as e:
                failed.append(
                    {"dataset_id": item["dataset_id"], "error": f"{type(e).__name__}: {e}"}
                )
                print(f"ERROR {item['dataset_id']} {type(e).__name__}: {e}", file=sys.stderr)
                continue
            if code != 0:
                failed.append(
                    {"dataset_id": item["dataset_id"], "error": f"local pipeline exited {code}"}
                )
                print(f"ERROR {item['dataset_id']} local pipeline exited {code}", file=sys.stderr)
                continue
            started.append({"dataset_id": item["dataset_id"], "execution": "local"})
            print(f"FINISHED local {item['dataset_id']}")
            continue
        try:
            result = start_validation_job(payload=payload)
        except Exception as e:
            failed.append({"dataset_id": item["dataset_id"], "error": f"{type(e).__name__}: {e}"})
            print(f"ERROR {item['dataset_id']} {type(e).__name__}: {e}", file=sys.stderr)
            continue
        execution = (result.get("execution_name") or "").rsplit("/", 1)[-1]
        started.append({"dataset_id": item["dataset_id"], "execution": execution or result.get("operation_name")})
        print(f"STARTED {item['dataset_id']} {execution or result.get('operation_name')}")

    if args.dry_run:
        return 0
    print(json.dumps({"started": started, "failed": failed, "skipped": skipped}, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
