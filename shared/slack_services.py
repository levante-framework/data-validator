import logging
from typing import Any

import requests
from google.api_core.exceptions import NotFound

import settings
from shared.secret_services import secret_service

logging.basicConfig(level=logging.INFO)


def _human_elapsed(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    try:
        s = float(seconds)
    except (TypeError, ValueError):
        return str(seconds)
    if s < 60:
        return f"{s:.1f}s"
    m, sec = divmod(int(s), 60)
    if m < 60:
        return f"{m}m {sec}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m {sec}s"


def _fmt_int(v: Any) -> str:
    if v is None:
        return "—"
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return str(v)


def format_data_validation_slack_summary(response: dict) -> str:
    """
    Readable Slack mrkdwn summary for a data_validation run (replaces raw JSON in notifications).
    """
    dp = response.get("dataset_parameters") or {}
    logs = response.get("logs") or {}
    stats = logs.get("total_validation_stats") or {}
    gcp = logs.get("gcp_logs") or {}
    red = logs.get("redivis_logs") or {}

    ds = dp.get("dataset_id", "—")
    ver = response.get("api_version", "—")
    elapsed = _human_elapsed(response.get("elapsed_time"))
    nvr = response.get("new_version_release")
    nvr_s = "yes" if nvr else "no"

    survey = stats.get("survey_responses") or {}
    orgs_block = dp.get("orgs") or ""
    if len(orgs_block) > 800:
        orgs_block = orgs_block[:800] + "…"

    validation_only = dp.get("is_save_to_storage") is False
    title = (
        "*Levante data validator* — validation only (no GCP/Redivis upload)"
        if validation_only
        else "*Levante data validator* — run finished"
    )
    lines = [
        title,
        "",
        f"*Dataset* `{ds}`",
        f"*API* v{ver} · *Runtime* {elapsed} · *New Redivis release* {nvr_s}",
        "",
        "*Pipeline*",
        f"• Save to storage: {dp.get('is_save_to_storage')} · Force Redivis: {dp.get('is_force_uploading_to_redivis')}",
        f"• Send Slack: {dp.get('send_slack', '—')}",
        "",
        "*Org scope*",
        orgs_block or "—",
        "",
        "*Validation totals*",
        f"• Users: {_fmt_int(stats.get('users', {}).get('valid_users'))} valid / {_fmt_int(stats.get('users', {}).get('total'))} total",
        f"• Runs: {_fmt_int(stats.get('runs', {}).get('valid_runs'))} valid / {_fmt_int(stats.get('runs', {}).get('total'))} total",
        f"• Trials: {_fmt_int(stats.get('trials', {}).get('valid_trials'))} valid / {_fmt_int(stats.get('trials', {}).get('total'))} total",
        f"• Cohorts: {_fmt_int(stats.get('cohorts'))} · Administrations: {_fmt_int(stats.get('administrations'))}",
        f"• Surveys — student: {_fmt_int(survey.get('student'))} · teacher: {_fmt_int(survey.get('teacher'))} · caregiver: {_fmt_int(survey.get('caregiver'))}",
        f"• Invalid rows: {_fmt_int(stats.get('invalid_data_count'))}",
    ]

    per_org = stats.get("orgs") or {}
    if isinstance(per_org, dict) and per_org:
        lines.append("")
        lines.append("*Per org*")
        for oid, ost in list(per_org.items())[:5]:
            u = ost.get("users", {})
            r = ost.get("runs", {})
            lines.append(
                f"• `{oid}` — users {_fmt_int(u.get('valid_users'))}/{_fmt_int(u.get('total'))}, "
                f"runs {_fmt_int(r.get('valid_runs'))}/{_fmt_int(r.get('total'))}"
            )
        if len(per_org) > 5:
            lines.append(f"• _…and {len(per_org) - 5} more org(s)_")

    if gcp:
        lines.extend(
            [
                "",
                "*GCP export*",
                f"• New version needed: {gcp.get('new_version_needed')}",
                f"• JSON blobs in bucket: {_fmt_int(gcp.get('blob_file_counts'))}",
                f"• Tables with row-count changes: {len(gcp.get('file_updated') or [])}",
                f"• Upload failures: {len(gcp.get('file_uploads_fail') or [])}",
                f"• Blob deletions: {len(gcp.get('file_deletion') or [])}",
            ]
        )

    if red:
        lines.extend(
            [
                "",
                "*Redivis*",
                f"• Tables: {_fmt_int(red.get('table_counts'))}",
                f"• Upload failures: {len(red.get('upload_fails') or [])}",
                f"• Dataset / version errors: {len(red.get('dataset_fails') or [])}",
                f"• Table deletions: {len(red.get('table_deletions') or [])}",
            ]
        )
    if validation_only:
        lines.extend(
            [
                "",
                "_Validation-only run: nothing uploaded. This response is not written to Firestore logs._",
            ]
        )
    else:
        lines.extend(
            ["", "_Full structured response is stored in Firestore `logs` for this dataset._"]
        )
    return "\n".join(lines)


def format_new_schemas_slack_summary(new_schemas: dict, *, dataset_id: str = "") -> str:
    """Compact Slack message when only new schema keys were detected."""
    lines = [
        "*Levante data validator* — new schema fields detected",
        "",
    ]
    if dataset_id:
        lines.append(f"*Dataset* `{dataset_id}`")
        lines.append("")
    for kind in ("runs", "trials", "surveys"):
        items = new_schemas.get(kind) or []
        if not items:
            continue
        lines.append(f"*{kind}* ({len(items)})")
        for item in items[:25]:
            lines.append(f"• {item}")
        if len(items) > 25:
            lines.append(f"• _…and {len(items) - 25} more_")
        lines.append("")
    if len(lines) <= 3:
        lines.append("_No schema details in payload._")
    return "\n".join(lines).strip()


def notify_slack(message: str, *, secret_id: str | None = None) -> None:
    """Post plain text to Slack via Incoming Webhook (secret in Secret Manager)."""
    default_sid = settings.config["SLACK_NOTIFICATION_WEB_HOOK"]
    hook_secret = (secret_id or default_sid).strip() or default_sid

    try:
        slack_web_hook_url = secret_service.get_secret_payload(secret_id=hook_secret)
    except NotFound:
        if hook_secret != default_sid:
            logging.warning(
                "Slack webhook secret %r not found; falling back to %r",
                hook_secret,
                default_sid,
            )
            slack_web_hook_url = secret_service.get_secret_payload(secret_id=default_sid)
        else:
            raise

    response = requests.post(slack_web_hook_url, json={"text": message})

    if response.status_code != 200:
        raise RuntimeError(f"Slack notification failed: {response.text}")
