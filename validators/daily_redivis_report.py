"""
End-of-day Redivis raw / processed version summary.

Runs at 1:30pm PT, after the per-site cron window (12:00–12:29 PT). Reads
today's Firestore logs. A newer Cloud Run execution that has not written a
log yet (still running, or failed before the log) overrides an earlier log.
Version text comes from that log. Posts one Slack message to
#daily-redivis-datasets-report.

Failures and version updates are listed by site. Sites with a finished run
and no raw or processed release are a count only. A site still executing at
report time is listed as still running, not as a failure.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import settings

PST = ZoneInfo("America/Los_Angeles")
_ERROR_LIMIT = 180


def _one_line(text: str, limit: int = _ERROR_LIMIT) -> str:
    collapsed = " ".join(str(text).split())
    if len(collapsed) <= limit:
        return collapsed
    if limit <= 1:
        return collapsed[:limit]
    return collapsed[: limit - 1] + "…"


def _join_errors(parts: list[str], limit: int = _ERROR_LIMIT) -> str | None:
    """Keep short trailing clauses when the notebook error is very long."""
    cleaned = [_one_line(part, limit) for part in parts if str(part).strip()]
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return cleaned[0]
    suffix = "; " + "; ".join(cleaned[1:])
    budget = limit - len(suffix)
    head = cleaned[0]
    if budget < 12:
        return _one_line(head + suffix, limit)
    if len(head) > budget:
        head = _one_line(head, budget)
    return head + suffix


def _as_utc(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    seconds = getattr(value, "seconds", None)
    if isinstance(seconds, int) and seconds > 0:
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    to_dt = getattr(value, "ToDatetime", None)
    if callable(to_dt):
        dt = to_dt()
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt if dt.year >= 2000 else None
    return None


def classify_site_log(data: dict | None) -> dict:
    """
    Classify one day's latest validation log.

    ``failed`` wins over a version update on the same run, so a notebook
    error after a raw release stays in the failure list.
    """
    result = {
        "status": "unchanged",
        "new_raw": False,
        "processed_released": False,
        "before_version": None,
        "after_version": None,
        "schema_only": False,
        "skip_reason": None,
        "error": None,
    }
    if not isinstance(data, dict) or not data:
        return result

    logs = data.get("logs") or {}
    process = data.get("process_dataset") or logs.get("process_dataset") or {}
    if not isinstance(process, dict):
        process = {}
    gcp = logs.get("gcp_logs") or {}
    red = logs.get("redivis_logs") or {}
    release = process.get("processed_release") or {}
    if not isinstance(release, dict):
        release = {}

    new_raw = bool(data.get("new_version_release"))
    processed_released = bool(release.get("released"))
    skip_reason = process.get("skip_reason") or None
    schema_only = skip_reason == "schema_only_raw"

    parts: list[str] = []
    if process.get("error"):
        parts.append(str(process["error"]))
    upload_fails = gcp.get("file_uploads_fail") or []
    if upload_fails:
        n = len(upload_fails) if isinstance(upload_fails, list) else 1
        parts.append(f"GCS upload failed ({n} file(s))")
    dataset_fails = red.get("dataset_fails") or []
    if dataset_fails:
        first = dataset_fails[0] if isinstance(dataset_fails, list) else dataset_fails
        parts.append(f"Redivis: {first}")

    result.update(
        {
            "new_raw": new_raw,
            "processed_released": processed_released,
            "before_version": release.get("before_version"),
            "after_version": release.get("after_version"),
            "schema_only": schema_only,
            "skip_reason": skip_reason,
            "error": _join_errors(parts),
        }
    )
    if parts:
        result["status"] = "failed"
    elif new_raw or processed_released:
        result["status"] = "updated"
    return result


def _tag(value) -> str:
    text = str(value).strip() if value is not None else ""
    return f"`{text}`" if text else ""


def version_phrase(row: dict) -> str:
    """One raw / processed version clause for a failed or updated site."""
    raw_tag = _tag(row.get("raw_version"))
    if row.get("new_raw"):
        raw_bit = f"raw {raw_tag} released".replace("  ", " ")
    elif raw_tag:
        raw_bit = f"raw {raw_tag} unchanged"
    else:
        raw_bit = "raw unchanged"

    if row.get("processed_released"):
        before = _tag(row.get("before_version")) or "—"
        after = _tag(row.get("after_version")) or "—"
        proc_bit = f"processed {before} → {after}"
    elif row.get("schema_only"):
        proc_bit = "processed skipped (schema-only)"
    elif row.get("skip_reason"):
        proc_bit = f"processed skipped ({row['skip_reason']})"
    elif row.get("new_raw") and not row.get("error"):
        proc_bit = "processed not released"
    else:
        proc_tag = _tag(row.get("before_version"))
        proc_bit = (
            f"processed {proc_tag} unchanged".replace("  ", " ")
            if proc_tag
            else "processed unchanged"
        )
    return f"{raw_bit} · {proc_bit}"


def format_daily_redivis_report(
    *,
    report_date: str,
    rows: list[dict],
    executions_available: bool = True,
) -> str:
    """High-level Slack mrkdwn. ``rows`` use bucket failed|running|updated|unchanged|no_run."""
    buckets: dict[str, list[dict]] = {
        "failed": [],
        "running": [],
        "updated": [],
        "unchanged": [],
        "no_run": [],
    }
    for row in rows:
        buckets.setdefault(row["bucket"], []).append(row)
    for group in buckets.values():
        group.sort(key=lambda r: (r.get("dataset_id") or "").lower())

    failed_n = len(buckets["failed"])
    running_n = len(buckets["running"])
    updated_n = len(buckets["updated"])
    unchanged_n = len(buckets["unchanged"])
    no_run_n = len(buckets["no_run"])

    counts = [
        f"{len(rows)} sites",
        f"{failed_n} failed",
        f"{updated_n} version updates",
        f"{unchanged_n} unchanged",
    ]
    if running_n:
        counts.insert(2, f"{running_n} still running")
    if no_run_n:
        counts.append(f"{no_run_n} no run")

    lines = [
        f"*Daily Redivis datasets* — {report_date} PT",
        " · ".join(counts),
        "",
        "*Failed*" if failed_n else "*Failed* — none",
    ]
    for row in buckets["failed"]:
        lines.append(f"• `{row['dataset_id']}` — {row.get('error') or 'failed'}")
        if row.get("show_versions"):
            lines.append(f"  {version_phrase(row)}")

    if running_n:
        lines.extend(["", "*Still running*"])
        lines.extend(f"• `{row['dataset_id']}`" for row in buckets["running"])

    lines.append("")
    if updated_n:
        lines.append("*Version updates*")
        for row in buckets["updated"]:
            lines.append(f"• `{row['dataset_id']}` — {version_phrase(row)}")
    else:
        lines.append("*Version updates* — none")

    unchanged_label = "site" if unchanged_n == 1 else "sites"
    lines.extend(["", f"*No version change* — {unchanged_n} {unchanged_label}"])

    if no_run_n:
        lines.extend(["", "*No run today*"])
        lines.extend(f"• `{row['dataset_id']}`" for row in buckets["no_run"])
        if not executions_available:
            lines.append(
                "_Cloud Run execution status was unavailable, so a site "
                "still running can appear here._"
            )
    return "\n".join(lines).strip()


def _log_doc_time(doc_id: str) -> datetime | None:
    """Firestore log doc ids are ``YYYY-MM-DD HH:MM:SS`` in PT."""
    try:
        return datetime.strptime(doc_id, "%Y-%m-%d %H:%M:%S").replace(tzinfo=PST)
    except (TypeError, ValueError):
        return None


def _execution_is_newer(
    execution: dict | None,
    log: dict | None,
    log_time: datetime | None,
) -> bool:
    """
    True when this execution has not been recorded by ``log``.

    A retry that is still running, or that failed before writing a log, starts
    after the earlier log. An undated running execution is treated as newer so
    a missing timestamp cannot hide it behind that log.
    """
    if not execution:
        return False
    if log is None:
        return True
    created = execution.get("created")
    if isinstance(created, datetime) and isinstance(log_time, datetime):
        return created > log_time
    return execution.get("state") == "running"


def resolve_site_outcome(
    log: dict | None,
    log_time: datetime | None,
    execution: dict | None,
) -> dict:
    """Pick the Slack bucket. A newer execution without a log wins."""
    if _execution_is_newer(execution, log, log_time):
        bucket, error = _outcome_without_log((execution or {}).get("state"))
        return {
            "bucket": bucket,
            "error": error,
            "from_log": False,
            "show_versions": False,
        }
    if log is not None:
        classified = classify_site_log(log)
        bucket = classified["status"]
        return {
            "bucket": bucket,
            "error": classified.get("error"),
            "from_log": True,
            "show_versions": bucket in ("failed", "updated"),
            **classified,
        }
    bucket, error = _outcome_without_log(None)
    return {
        "bucket": bucket,
        "error": error,
        "from_log": False,
        "show_versions": False,
    }


def note_site_execution(
    latest: dict[str, dict],
    *,
    dataset_id: str,
    created: datetime | None,
    state: str,
    start_utc: datetime,
) -> None:
    """Keep the newest execution at or after ``start_utc``. Order does not matter."""
    if not dataset_id or created is None or created < start_utc:
        return
    prev = latest.get(dataset_id)
    if prev is None or created > prev["created"]:
        latest[dataset_id] = {"state": state, "created": created}


def _latest_log_for_date(
    dataset_id: str, date_str: str
) -> tuple[dict | None, datetime | None]:
    from shared.firestore_services import firestore_services

    base = firestore_services.admin_db.collection("logs").document(dataset_id)
    try:
        docs = list(base.collection(date_str).get())
    except Exception as e:
        logging.warning(
            "daily_redivis_report: log read %s/%s failed: %s",
            dataset_id,
            date_str,
            e,
        )
        return None, None
    if not docs:
        return None, None
    snap = max(docs, key=lambda d: d.id)
    data = snap.to_dict() or {}
    if not isinstance(data, dict):
        return None, _log_doc_time(snap.id)
    return data, _log_doc_time(snap.id)


def _payload_from_execution(execution) -> dict | None:
    template = getattr(execution, "template", None)
    containers = getattr(template, "containers", None) or []
    for container in containers:
        for env in getattr(container, "env", None) or []:
            if getattr(env, "name", None) != "DATA_VALIDATOR_PAYLOAD":
                continue
            raw = getattr(env, "value", None)
            if not raw:
                return None
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return None
            return parsed if isinstance(parsed, dict) else None
    return None


def _outcome_without_log(state: str | None) -> tuple[str, str | None]:
    """Bucket and error when a site has no Firestore log for the report date."""
    if state == "running":
        return "running", None
    if state == "failed":
        return "failed", "Cloud Run execution failed before a log was written"
    if state == "succeeded":
        return "failed", "Cloud Run execution finished with no Firestore log"
    return "no_run", None


def _execution_state(execution) -> str:
    completed = _as_utc(getattr(execution, "completion_time", None))
    succeeded = int(getattr(execution, "succeeded_count", 0) or 0)
    failed = int(getattr(execution, "failed_count", 0) or 0)
    cancelled = int(getattr(execution, "cancelled_count", 0) or 0)
    running = int(getattr(execution, "running_count", 0) or 0)
    if completed is None and (running or getattr(execution, "reconciling", False)):
        return "running"
    if completed is None and succeeded == 0 and failed == 0 and cancelled == 0:
        return "running"
    if failed or cancelled:
        return "failed"
    if succeeded:
        return "succeeded"
    return "failed"


def _today_site_executions(
    raw_ids: set[str], start_utc: datetime
) -> tuple[dict[str, dict], bool]:
    """
    Latest today execution per site raw id.

    Returns ``(dataset_id → {state, created}, available)``. Every execution is
    checked against ``start_utc``; list order is not assumed.
    """
    project_id = os.getenv("project_id")
    if not project_id or not raw_ids:
        return {}, bool(project_id)
    parent = (
        f"projects/{project_id}/locations/"
        f"{settings.config['CLOUD_RUN_JOB_REGION']}/jobs/"
        f"{settings.config['CLOUD_RUN_JOB_NAME']}"
    )
    try:
        from google.cloud import run_v2

        client = run_v2.ExecutionsClient()
        executions = client.list_executions(parent=parent)
    except Exception as e:
        logging.warning("daily_redivis_report: list executions failed: %s", e)
        return {}, False

    latest: dict[str, dict] = {}
    try:
        for execution in executions:
            payload = _payload_from_execution(execution)
            if not payload:
                continue
            operation = payload.get("operation") or "data_validation"
            if operation != "data_validation":
                continue
            dataset_id = str(payload.get("dataset_id") or "").strip()
            if dataset_id not in raw_ids:
                continue
            note_site_execution(
                latest,
                dataset_id=dataset_id,
                created=_as_utc(getattr(execution, "create_time", None)),
                state=_execution_state(execution),
                start_utc=start_utc,
            )
    except Exception as e:
        logging.warning("daily_redivis_report: reading executions failed: %s", e)
        return latest, False
    return latest, True


def run_daily_redivis_datasets_report(
    *,
    dry_run: bool = False,
    now_pst: datetime | None = None,
) -> dict[str, Any]:
    """Build the daily summary and post it unless ``dry_run`` is true."""
    from google.api_core.exceptions import NotFound

    from shared.secret_services import secret_service
    from shared.slack_services import notify_slack
    from validators.weekly_report import list_active_sites

    if now_pst is None:
        now_pst = datetime.now(PST)
    elif now_pst.tzinfo is None:
        now_pst = now_pst.replace(tzinfo=PST)
    else:
        now_pst = now_pst.astimezone(PST)
    report_date = now_pst.strftime("%Y-%m-%d")
    start_utc = now_pst.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(
        timezone.utc
    )

    sites = list_active_sites()
    raw_ids = {
        (s.get("log_dataset_name") or "").strip()
        for s in sites
        if (s.get("log_dataset_name") or "").strip()
    }
    executions, executions_available = _today_site_executions(raw_ids, start_utc)

    rows: list[dict] = []
    for site in sites:
        raw_id = (site.get("log_dataset_name") or "").strip()
        processed_id = (site.get("dataset_name") or "").strip()
        if not raw_id:
            continue
        log, log_time = _latest_log_for_date(raw_id, report_date)
        outcome = resolve_site_outcome(log, log_time, executions.get(raw_id))
        row = {
            "dataset_id": raw_id,
            "processed_id": processed_id,
            "new_raw": False,
            "processed_released": False,
            "before_version": None,
            "after_version": None,
            "schema_only": False,
            "skip_reason": None,
            **outcome,
        }
        rows.append(row)

    message = format_daily_redivis_report(
        report_date=report_date,
        rows=rows,
        executions_available=executions_available,
    )
    counts = {
        "sites": len(rows),
        "failed": sum(1 for r in rows if r["bucket"] == "failed"),
        "running": sum(1 for r in rows if r["bucket"] == "running"),
        "updated": sum(1 for r in rows if r["bucket"] == "updated"),
        "unchanged": sum(1 for r in rows if r["bucket"] == "unchanged"),
        "no_run": sum(1 for r in rows if r["bucket"] == "no_run"),
    }
    secret_id = (
        settings.config.get("SLACK_DAILY_REDIVIS_DATASETS_REPORT_WEBHOOK_SECRET_ID")
        or ""
    ).strip()
    logging.info(
        "daily_redivis_report: date=%s dry_run=%s counts=%s",
        report_date,
        dry_run,
        counts,
    )

    slack_posted = False
    slack_error = None
    if dry_run:
        slack_error = "dry_run=true"
    elif not secret_id:
        slack_error = "slack webhook secret id is empty"
    else:
        try:
            secret_service.get_secret_payload(secret_id=secret_id)
        except NotFound:
            slack_error = f"secret {secret_id} not found"
            logging.error("daily_redivis_report: %s", slack_error)
        else:
            try:
                notify_slack(message, secret_id=secret_id)
                slack_posted = True
            except Exception as e:
                slack_error = str(e)[:200]
                logging.error("daily_redivis_report: slack post failed: %s", e)

    return {
        "report_date": report_date,
        "dry_run": dry_run,
        "counts": counts,
        "slack_posted": slack_posted,
        "slack_webhook_secret_id": secret_id,
        "slack_error": slack_error,
        "executions_available": executions_available,
        "message_preview": message,
    }
