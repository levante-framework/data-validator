"""
Weekly LEVANTE data-validator summary report.

Runs Monday 8am PST via Cloud Scheduler. Aggregates per-site activity,
schema drift, scheduler health, crash alerts, and validation health for the
previous calendar week (Mon 00:00 PST → Sun 23:59 PST), then posts a single
Slack message.

Data sources:
- Per-site activity: existing `logs/{dataset_id}/{YYYY-MM-DD}/{run-ts}` docs
  written by `firestore_services.set_logs_to_firebase`. Baseline-vs-current
  diff of `total_validation_stats` gives weekly growth (users by role,
  runs, trials, surveys by role, invalid). Not Redivis row counts.
- New administrations: live Firestore `administrations` docs whose
  `dateOpened` or `createdAt` falls in the week (not the validator log
  administration count, which is a cumulative export snapshot).
- Redivis state: `RedivisServices.get_current_dataset_status()` per site
  (released vs awaiting). Do not treat this as weekly activity.
- Redivis state: `RedivisServices.get_current_dataset_status()` per site.
- Schema drift: sample recent docs per top-level collection; for `users`,
  stratify by `userType` so every role is represented. Also sample
  `runs`, `trials`, and `surveyResponses` via collection-group queries.
  Diff {fields, subcollections} vs the prior week's snapshot at
  `logs/_weekly_state/snapshot:{week_iso}`.
- Scheduler health + function crashes: Cloud Logging entries for the
  current GCP project (the one the function is running in).
- Awaiting Redivis releases: existing logic from `redivis_individual_release`.
"""

from __future__ import annotations

import logging
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from google.cloud import firestore as fs
from google.cloud import logging as gcl

import settings
from shared.airtable_services import AirtableServices
from shared.firestore_services import firestore_services
from shared.slack_services import notify_slack
from validators.redivis_services import RedivisServices


PST = ZoneInfo("America/Los_Angeles")


# Canonical "updated" timestamp per top-level Firestore collection. Confirmed
# empirically May 2026 — many collections lack `lastUpdated` so we don't blindly
# assume it.
COLLECTION_UPDATED_FIELD: dict[str, str] = {
    "districts":       "updatedAt",
    "groups":          "updatedAt",
    "schools":         "updatedAt",
    "classes":         "updatedAt",
    "administrations": "updatedAt",
    "tasks":           "lastUpdated",
    "users":           "lastUpdated",
    "guests":          "updatedAt",
}

# Top-level collections used by the validator we care about for schema drift.
SCHEMA_DRIFT_COLLECTIONS = list(COLLECTION_UPDATED_FIELD.keys())

# How many recent docs to sample per top-level collection. Larger = more reliable
# field-presence detection at the cost of more Firestore reads.
SCHEMA_SNAPSHOT_SAMPLE = 20

# Stratified `users` sampling: N recent docs per userType (not one global slice).
USERS_SCHEMA_TYPES = ("student", "teacher", "parent", "test", "admin")
SCHEMA_USERS_PER_TYPE = 4

# Collection-group samples under `users/*`. Multiple order fields on
# surveyResponses cover legacy (createdAt) and run-like (timeStarted) shapes.
SCHEMA_SUBCOLLECTION_SAMPLE = 10
# Example Firestore ids attached to each drifted field (from the existing sample).
SCHEMA_EXAMPLE_IDS = 3
USER_SUBCOLLECTION_SCHEMA_SPECS: list[tuple[str, str, list[str]]] = [
    ("users/runs", "runs", ["timeStarted"]),
    ("users/trials", "trials", ["serverTimestamp"]),
    ("users/surveyResponses", "surveyResponses", ["createdAt", "timeStarted"]),
]


# ----------------------------------------------------------------------------
# Time windowing
# ----------------------------------------------------------------------------

def calendar_week_window_pst(now_pst: datetime | None = None) -> tuple[datetime, datetime, str]:
    """
    Return (start_utc, end_utc, iso_week_label) for the *previous* calendar
    week in PST. `start_utc` is Monday 00:00 PST converted to UTC; `end_utc`
    is the last microsecond of Sunday 23:59 PST converted to UTC. The label
    is the ISO week of the start date, e.g. "2026-W20".
    """
    if now_pst is None:
        now_pst = datetime.now(PST)
    # Monday of the current week, 00:00 PST.
    this_mon_pst = (now_pst - timedelta(days=now_pst.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    prev_mon_pst = this_mon_pst - timedelta(days=7)
    end_pst = this_mon_pst - timedelta(microseconds=1)
    iso = prev_mon_pst.strftime("%G-W%V")
    return prev_mon_pst.astimezone(timezone.utc), end_pst.astimezone(timezone.utc), iso


# ----------------------------------------------------------------------------
# Site discovery
# ----------------------------------------------------------------------------

def list_active_sites() -> list[dict]:
    """
    Pull every Airtable row with `Redivis individual` checked and return the
    subset with a real Firestore siteId (i.e. not `missing_site_id` or empty).
    The validator writes logs under the raw Redivis dataset id, while the
    unmarked Name is retained as the human-facing report label.
    """
    individual_field = settings.config["AIRTABLE_FIELD_REDIVIS_INDIVIDUAL"]
    name_field = settings.config["AIRTABLE_FIELD_REDIVIS_DATASET_NAME"]
    raw_name_field = settings.config["AIRTABLE_FIELD_REDIVIS_NAME"]
    site_field = settings.config["AIRTABLE_FIELD_FIRESTORE_SITE_ID"]
    placeholder = settings.config.get("MISSING_SITE_ID_PLACEHOLDER", "missing_site_id")
    raw_suffix = settings.config.get("RAW_DATASET_SUFFIX", "-raw")

    at = AirtableServices()
    try:
        rows = at.list_dataset_records(formula=f"{{{individual_field}}}=1")
    except Exception:
        rows = at.list_dataset_records()

    out = []
    for r in rows:
        fields = r.get("fields") or {}
        if not fields.get(individual_field):
            continue
        name = (fields.get(name_field) or "").strip()
        site_id = (fields.get(site_field) or "").strip()
        if not name or not site_id or site_id == placeholder:
            continue
        raw_name = (fields.get(raw_name_field) or "").strip() or f"{name}{raw_suffix}"
        out.append({
            "dataset_name": name,
            "log_dataset_name": raw_name,
            "site_id": site_id,
        })
    return out


# ----------------------------------------------------------------------------
# Per-site activity from existing daily log docs
# ----------------------------------------------------------------------------

def _to_utc_datetime(value) -> datetime | None:
    if value is None:
        return None
    if hasattr(value, "timestamp"):
        return datetime.fromtimestamp(value.timestamp(), tz=timezone.utc)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _in_window(value, start_utc: datetime, end_utc: datetime) -> bool:
    ts = _to_utc_datetime(value)
    return bool(ts and start_utc <= ts <= end_utc)


def _delta_nonneg(cur: dict, base: dict, key: str) -> int:
    return max((cur.get(key) or 0) - (base.get(key) or 0), 0)


def _empty_site_activity(**extra) -> dict:
    row = {
        "users": 0,
        "children": 0,
        "teachers": 0,
        "caregivers": 0,
        "runs": 0,
        "trials": 0,
        "surveys": 0,
        "surveys_children": 0,
        "surveys_teachers": 0,
        "surveys_caregivers": 0,
        "invalid": 0,
        "has_user_roles": False,
        "note": None,
    }
    row.update(extra)
    return row


def _stats_from_log(log_doc) -> dict:
    d = log_doc.to_dict() or {}
    stats = (d.get("logs") or {}).get("total_validation_stats") or {}
    users = stats.get("users") or {}
    runs = stats.get("runs") or {}
    trials = stats.get("trials") or {}
    surveys = stats.get("survey_responses") or {}
    surveys_student = surveys.get("student") or 0
    surveys_teacher = surveys.get("teacher") or 0
    surveys_caregiver = surveys.get("caregiver") or 0
    has_user_roles = any(
        k in users for k in ("student", "teacher", "caregiver")
    )
    return {
        "users_total": users.get("total", 0) or 0,
        "users_valid": users.get("valid_users", 0) or 0,
        "users_student": users.get("student") or 0,
        "users_teacher": users.get("teacher") or 0,
        "users_caregiver": users.get("caregiver") or 0,
        "has_user_roles": has_user_roles,
        "runs_total": runs.get("total", 0) or 0,
        "trials_total": trials.get("total", 0) or 0,
        "surveys_student": surveys_student,
        "surveys_teacher": surveys_teacher,
        "surveys_caregiver": surveys_caregiver,
        "survey_responses_total": (
            surveys_student + surveys_teacher + surveys_caregiver
        ),
        "invalid_data_count": stats.get("invalid_data_count", 0) or 0,
        "_doc_path": log_doc.reference.path,
    }


def _activity_from_stats(cur: dict, base: dict | None) -> dict:
    if base is None:
        base = {}
        first = True
    else:
        first = False
    surveys_children = (
        cur.get("surveys_student", 0)
        if first
        else _delta_nonneg(cur, base, "surveys_student")
    )
    surveys_teachers = (
        cur.get("surveys_teacher", 0)
        if first
        else _delta_nonneg(cur, base, "surveys_teacher")
    )
    surveys_caregivers = (
        cur.get("surveys_caregiver", 0)
        if first
        else _delta_nonneg(cur, base, "surveys_caregiver")
    )
    has_roles = bool(cur.get("has_user_roles")) and (
        first or bool(base.get("has_user_roles"))
    )
    if has_roles:
        children = (
            cur.get("users_student", 0)
            if first
            else _delta_nonneg(cur, base, "users_student")
        )
        teachers = (
            cur.get("users_teacher", 0)
            if first
            else _delta_nonneg(cur, base, "users_teacher")
        )
        caregivers = (
            cur.get("users_caregiver", 0)
            if first
            else _delta_nonneg(cur, base, "users_caregiver")
        )
        users = children + teachers + caregivers
    else:
        children = teachers = caregivers = 0
        users = (
            cur.get("users_total", 0)
            if first
            else _delta_nonneg(cur, base, "users_total")
        )
    runs = (
        cur.get("runs_total", 0)
        if first
        else _delta_nonneg(cur, base, "runs_total")
    )
    trials = (
        cur.get("trials_total", 0)
        if first
        else _delta_nonneg(cur, base, "trials_total")
    )
    return {
        "users": users,
        "children": children,
        "teachers": teachers,
        "caregivers": caregivers,
        "runs": runs,
        "trials": trials,
        "surveys": surveys_children + surveys_teachers + surveys_caregivers,
        "surveys_children": surveys_children,
        "surveys_teachers": surveys_teachers,
        "surveys_caregivers": surveys_caregivers,
        "invalid": cur.get("invalid_data_count", 0) or 0,
        "has_user_roles": has_roles,
    }


def _find_logs_for_dataset(
    dataset_id: str, start_utc: datetime, end_utc: datetime
) -> tuple[Any, Any]:
    """
    Return (baseline_log_snap, current_log_snap):
      - baseline = latest log doc with date < window start
      - current  = latest log doc with date <= window end (within window if possible)
    """
    base = (firestore_services.admin_db
            .collection("logs").document(dataset_id))
    try:
        date_subs = sorted(c.id for c in base.collections())
    except Exception as e:
        logging.warning("weekly_report: could not list log subcolls for %s: %s",
                        dataset_id, e)
        return None, None
    if not date_subs:
        return None, None

    start_pst_str = start_utc.astimezone(PST).strftime("%Y-%m-%d")
    end_pst_str = end_utc.astimezone(PST).strftime("%Y-%m-%d")

    baseline = None
    current = None
    for date_str in date_subs:
        # Each date subcollection usually has 1–3 docs (cron + retries). Just
        # fetch them and pick the alphabetically-last one — doc ids are
        # `YYYY-MM-DD HH:MM:SS` so string sort == chronological sort. This
        # avoids any order_by(__name__) quirks observed empirically.
        try:
            docs_in_day = list(base.collection(date_str).get())
        except Exception as e:
            logging.warning(
                "weekly_report: failed reading %s/%s: %s",
                dataset_id, date_str, e,
            )
            continue
        if not docs_in_day:
            continue
        snap = max(docs_in_day, key=lambda d: d.id)
        if date_str < start_pst_str:
            baseline = snap          # keep updating as we walk forward
        elif start_pst_str <= date_str <= end_pst_str:
            current = snap           # keep updating as we walk forward
    return baseline, current


def collect_firestore_activity(
    sites: list[dict], start_utc: datetime, end_utc: datetime
) -> dict:
    """
    Per-site week-over-week growth using existing daily log docs.
    Returns:
      {
        "per_site": {dataset_id: {children, teachers, caregivers, runs, ...}},
        "totals": {children, teachers, caregivers, runs, trials, surveys_*, invalid},
        "missing_baseline": [dataset_id, ...],
        "no_logs_at_all":   [dataset_id, ...],
      }
    """
    per_site: dict[str, dict] = {}
    totals = Counter()
    missing_baseline: list[str] = []
    no_logs_at_all: list[str] = []

    for s in sites:
        ds = s["dataset_name"]
        log_ds = s.get("log_dataset_name") or ds
        baseline, current = _find_logs_for_dataset(log_ds, start_utc, end_utc)
        if log_ds != ds and (not baseline or not current):
            # Preserve continuity across the processed-name → raw-name log-key
            # migration. Prefer raw logs, but fill either side of the weekly
            # comparison from the former unmarked key when necessary.
            legacy_baseline, legacy_current = _find_logs_for_dataset(
                ds, start_utc, end_utc
            )
            baseline = baseline or legacy_baseline
            current = current or legacy_current
        if not current and not baseline:
            no_logs_at_all.append(ds)
            per_site[ds] = _empty_site_activity(
                note="no_logs_in_or_before_window",
                log_dataset_name=log_ds,
            )
            continue
        if not baseline:
            # First-time run inside this window — treat all current totals as
            # growth (subject to confirmation).
            cur = _stats_from_log(current) if current else {}
            per_site[ds] = _activity_from_stats(cur, None)
            per_site[ds]["note"] = "first_run_no_baseline"
            per_site[ds]["log_dataset_name"] = log_ds
            missing_baseline.append(ds)
            for k, v in per_site[ds].items():
                if isinstance(v, int) and not isinstance(v, bool):
                    totals[k] += v
            continue
        if not current:
            # No log in window — site likely had no cron-detected change.
            per_site[ds] = _empty_site_activity(
                note="no_logs_in_window",
                log_dataset_name=log_ds,
            )
            continue
        b = _stats_from_log(baseline)
        c = _stats_from_log(current)
        per_site[ds] = _activity_from_stats(c, b)
        per_site[ds]["note"] = None
        per_site[ds]["log_dataset_name"] = log_ds
        for k, v in per_site[ds].items():
            if isinstance(v, int) and not isinstance(v, bool):
                totals[k] += v

    return {
        "per_site": per_site,
        "totals": dict(totals),
        "missing_baseline": missing_baseline,
        "no_logs_at_all": no_logs_at_all,
    }


def collect_new_administrations(
    sites: list[dict], start_utc: datetime, end_utc: datetime
) -> dict:
    """
    Firestore administrations whose ``dateOpened`` or ``createdAt`` falls in
    the week. Count is unique by administration_id across sites.
    """
    seen: dict[str, dict] = {}
    for s in sites:
        site_id = s.get("site_id")
        if not site_id:
            continue
        for admin in firestore_services.iter_administrations_for_site(site_id):
            opened = admin.get("dateOpened")
            created = admin.get("createdAt")
            if not (
                _in_window(opened, start_utc, end_utc)
                or _in_window(created, start_utc, end_utc)
            ):
                continue
            aid = admin.get("administration_id")
            if not aid or aid in seen:
                continue
            when = _to_utc_datetime(opened) or _to_utc_datetime(created)
            seen[aid] = {
                "administration_id": aid,
                "name": (admin.get("name") or "").strip() or "(no name)",
                "site": s.get("dataset_name"),
                "when": when,
            }
    items = sorted(
        seen.values(),
        key=lambda x: x["when"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return {
        "count": len(items),
        "items": items,
    }


# ----------------------------------------------------------------------------
# Redivis state
# ----------------------------------------------------------------------------

def collect_redivis_state(sites: list[dict]) -> dict:
    """For each site, current Redivis dataset existence/release state."""
    rs = RedivisServices()
    awaiting: list[str] = []
    versions: dict[str, dict] = {}
    for s in sites:
        ds = s["dataset_name"]
        try:
            rs.set_dataset(dataset_id=ds)
            st = rs.get_current_dataset_status()
        except Exception as e:
            logging.warning("weekly_report: redivis lookup failed for %s: %s",
                            ds, e)
            st = {"exists": None, "is_released": None, "version_tag": None}
        versions[ds] = st
        if not (st.get("exists") and st.get("is_released")):
            awaiting.append(ds)
    return {"versions": versions, "awaiting": awaiting}


# ----------------------------------------------------------------------------
# Cloud Logging — scheduler health + crash alerts
# ----------------------------------------------------------------------------

def _logging_client() -> gcl.Client | None:
    """Cloud Logging client scoped to the current GCP project (where the
    function is running). Returns None if the project id can't be determined."""
    project_id = os.getenv("project_id")
    if not project_id:
        logging.warning("weekly_report: project_id env not set; skipping log queries")
        return None
    try:
        return gcl.Client(project=project_id)
    except Exception as e:
        logging.warning("weekly_report: failed to init logging client: %s", e)
        return None


def collect_scheduler_health(start_utc: datetime, end_utc: datetime) -> dict:
    """
    Count scheduler attempt failures in the window, grouped by job_id.
    Failures we care about:
      - URL_UNREACHABLE-*
      - URL_REJECTED-*
      - status != "OK"
    """
    client = _logging_client()
    if client is None:
        return {"by_job": {}, "error": "no_logging_client"}

    filter_str = (
        f'resource.type="cloud_scheduler_job" '
        f'timestamp>="{start_utc.isoformat()}" '
        f'timestamp<="{end_utc.isoformat()}" '
        f'jsonPayload."@type"="type.googleapis.com/google.cloud.scheduler.logging.AttemptFinished" '
        f'(jsonPayload.status!="OK" OR severity>=ERROR)'
    )
    by_job: dict[str, dict] = defaultdict(lambda: {"count": 0, "kinds": Counter()})
    try:
        for entry in client.list_entries(filter_=filter_str, max_results=2000):
            payload = entry.payload or {}
            if not isinstance(payload, dict):
                continue
            job = (payload.get("jobName") or "").split("/")[-1] or "?"
            debug = payload.get("debugInfo") or payload.get("status") or "UNKNOWN"
            kind = str(debug).split(".")[0]
            by_job[job]["count"] += 1
            by_job[job]["kinds"][kind] += 1
    except Exception as e:
        logging.warning("weekly_report: scheduler logging query failed: %s", e)
        return {"by_job": {}, "error": str(e)[:200]}
    return {
        "by_job": {
            j: {"count": v["count"], "kinds": dict(v["kinds"])}
            for j, v in by_job.items()
        },
        "error": None,
    }


def collect_crash_alerts(start_utc: datetime, end_utc: datetime) -> dict:
    """Count function-level crashes the main.py top-level except produced."""
    client = _logging_client()
    if client is None:
        return {"count": 0, "samples": [], "error": "no_logging_client"}
    filter_str = (
        f'resource.type="cloud_run_revision" '
        f'resource.labels.service_name="data-validator" '
        f'timestamp>="{start_utc.isoformat()}" '
        f'timestamp<="{end_utc.isoformat()}" '
        f'textPayload:"data_validation crashed for dataset_id="'
    )
    count = 0
    samples: list[str] = []
    try:
        for entry in client.list_entries(filter_=filter_str, max_results=200):
            count += 1
            if len(samples) < 5:
                tp = entry.payload if isinstance(entry.payload, str) else str(entry.payload)
                samples.append(tp[:160])
    except Exception as e:
        logging.warning("weekly_report: crash log query failed: %s", e)
        return {"count": 0, "samples": [], "error": str(e)[:200]}
    return {"count": count, "samples": samples, "error": None}


# ----------------------------------------------------------------------------
# Validation health — aggregated from daily log docs
# ----------------------------------------------------------------------------

def collect_validation_health(
    sites: list[dict], start_utc: datetime, end_utc: datetime
) -> dict:
    """
    Aggregate `invalid_data_count` and `new_schemas` from every log doc in
    the window across all sites.
    """
    invalid_by_site: dict[str, int] = {}
    new_schemas_by_kind: defaultdict[str, Counter] = defaultdict(Counter)
    api_versions: Counter[str] = Counter()
    runs_count = 0
    for s in sites:
        ds = s["dataset_name"]
        log_ds = s.get("log_dataset_name") or ds
        base = (firestore_services.admin_db
                .collection("logs").document(log_ds))
        try:
            date_subs = sorted(c.id for c in base.collections())
        except Exception:
            continue
        start_pst_str = start_utc.astimezone(PST).strftime("%Y-%m-%d")
        end_pst_str = end_utc.astimezone(PST).strftime("%Y-%m-%d")
        latest_invalid = 0
        latest_seen = False
        for date_str in date_subs:
            if not (start_pst_str <= date_str <= end_pst_str):
                continue
            for doc in base.collection(date_str).get():
                runs_count += 1
                d = doc.to_dict() or {}
                api_version = d.get("api_version")
                if api_version:
                    api_versions[str(api_version)] += 1
                stats = (d.get("logs") or {}).get("total_validation_stats") or {}
                latest_invalid = stats.get("invalid_data_count", 0) or 0
                latest_seen = True
                ns = stats.get("new_schemas") or {}
                for k, items in ns.items():
                    if not items:
                        continue
                    for item in items:
                        new_schemas_by_kind[k][str(item)] += 1
        if latest_seen:
            invalid_by_site[ds] = latest_invalid
    return {
        "invalid_by_site": invalid_by_site,
        "new_schemas": {k: dict(v) for k, v in new_schemas_by_kind.items()},
        "api_versions": dict(api_versions),
        "log_docs_scanned": runs_count,
    }


# ----------------------------------------------------------------------------
# Schema drift detection (Firebase)
# ----------------------------------------------------------------------------

def _schema_locator(doc) -> str:
    """Id an admin can look up: userId for users/* subdocs, otherwise the doc id."""
    parts = (getattr(getattr(doc, "reference", None), "path", None) or "").split("/")
    if len(parts) >= 4 and parts[0] == "users":
        return parts[1]
    return getattr(doc, "id", "") or ""


def _record_example(bucket: dict[str, list[str]], key: str, locator: str) -> None:
    if not key or not locator:
        return
    ids = bucket.setdefault(key, [])
    if locator not in ids and len(ids) < SCHEMA_EXAMPLE_IDS:
        ids.append(locator)


def _merge_example_maps(
    dst: dict[str, list[str]], src: dict[str, list[str]] | None
) -> dict[str, list[str]]:
    for key, ids in (src or {}).items():
        for locator in ids:
            _record_example(dst, key, locator)
    return dst


def _fingerprint_docs(
    docs: list,
) -> tuple[set[str], set[str], Counter[str], Counter[str], dict[str, list[str]], dict[str, list[str]]]:
    """Field/subcollection unions, counts, and up to 3 example locators per name."""
    fields: set[str] = set()
    subcolls: set[str] = set()
    field_counts: Counter[str] = Counter()
    subcoll_counts: Counter[str] = Counter()
    field_examples: dict[str, list[str]] = {}
    subcoll_examples: dict[str, list[str]] = {}
    for d in docs:
        locator = _schema_locator(d)
        body = d.to_dict() or {}
        fields.update(body.keys())
        field_counts.update(body.keys())
        for key in body:
            _record_example(field_examples, key, locator)
        try:
            for sub in d.reference.collections():
                subcolls.add(sub.id)
                subcoll_counts[sub.id] += 1
                _record_example(subcoll_examples, sub.id, locator)
        except Exception:
            pass
    return fields, subcolls, field_counts, subcoll_counts, field_examples, subcoll_examples


def _query_recent_docs(
    query,
    *,
    coll_label: str,
    order_field: str | None,
    sample: int,
) -> list:
    """Run order_by+limit when possible; fall back to unordered limit."""
    if order_field:
        try:
            return list(
                query.order_by(order_field, direction=fs.Query.DESCENDING)
                     .limit(sample)
                     .get()
            )
        except Exception as e:
            logging.warning(
                "weekly_report: order_by %s failed on %s (%s); using unordered sample",
                order_field, coll_label, e,
            )
    try:
        return list(query.limit(sample).get())
    except Exception as e:
        logging.warning("weekly_report: sample failed on %s: %s", coll_label, e)
        return []


def _schema_entry(
    fields: set[str],
    subcolls: set[str],
    sample_size: int,
    *,
    field_counts: Counter[str] | None = None,
    subcollection_counts: Counter[str] | None = None,
    field_examples: dict[str, list[str]] | None = None,
    subcollection_examples: dict[str, list[str]] | None = None,
    **extra,
) -> dict:
    out: dict[str, Any] = {
        "fields": sorted(fields),
        "subcollections": sorted(subcolls),
        "sample_size": sample_size,
        "field_counts": dict(field_counts or {}),
        "subcollection_counts": dict(subcollection_counts or {}),
        "field_examples": field_examples or {},
        "subcollection_examples": subcollection_examples or {},
    }
    out.update(extra)
    return out


def _sample_collection_for_schema(
    coll_name: str, updated_field: str, sample: int = SCHEMA_SNAPSHOT_SAMPLE
) -> dict:
    """
    Capture a fingerprint of one top-level collection:
      - fields: union of top-level keys across the latest N docs
      - subcollections: union of subcollection names across the latest N docs
    Falls back to unordered limit if the order_by fails.
    """
    coll = firestore_services.admin_db.collection(coll_name)
    docs = _query_recent_docs(
        coll, coll_label=coll_name, order_field=updated_field, sample=sample,
    )
    fields, subcolls, field_counts, subcoll_counts, field_examples, subcoll_examples = (
        _fingerprint_docs(docs)
    )
    return _schema_entry(
        fields,
        subcolls,
        len(docs),
        field_counts=field_counts,
        subcollection_counts=subcoll_counts,
        field_examples=field_examples,
        subcollection_examples=subcoll_examples,
    )


def _sample_users_schema_stratified(updated_field: str) -> dict[str, dict]:
    """
    Sample recent user docs per userType so schema drift is not skewed by one
    role dominating lastUpdated. Returns aggregate `users` plus `users/{type}`
    entries for per-role diffs.
    """
    coll = firestore_services.admin_db.collection("users")
    all_fields: set[str] = set()
    all_subcolls: set[str] = set()
    all_field_counts: Counter[str] = Counter()
    all_subcoll_counts: Counter[str] = Counter()
    all_field_examples: dict[str, list[str]] = {}
    all_subcoll_examples: dict[str, list[str]] = {}
    fields_by_type: dict[str, list[str]] = {}
    dist: Counter[str] = Counter()
    total = 0
    snap: dict[str, dict] = {}

    for user_type in USERS_SCHEMA_TYPES:
        docs = _query_recent_docs(
            coll.where("userType", "==", user_type),
            coll_label=f"users(userType={user_type})",
            order_field=updated_field,
            sample=SCHEMA_USERS_PER_TYPE,
        )
        if not docs:
            continue
        fields, subcolls, field_counts, subcoll_counts, field_examples, subcoll_examples = (
            _fingerprint_docs(docs)
        )
        all_fields |= fields
        all_subcolls |= subcolls
        all_field_counts.update(field_counts)
        all_subcoll_counts.update(subcoll_counts)
        _merge_example_maps(all_field_examples, field_examples)
        _merge_example_maps(all_subcoll_examples, subcoll_examples)
        fields_by_type[user_type] = sorted(fields)
        dist[user_type] = len(docs)
        total += len(docs)
        snap[f"users/{user_type}"] = _schema_entry(
            fields,
            subcolls,
            len(docs),
            field_counts=field_counts,
            subcollection_counts=subcoll_counts,
            field_examples=field_examples,
            subcollection_examples=subcoll_examples,
        )

    snap["users"] = _schema_entry(
        all_fields, all_subcolls, total,
        field_counts=all_field_counts,
        subcollection_counts=all_subcoll_counts,
        field_examples=all_field_examples,
        subcollection_examples=all_subcoll_examples,
        fields_by_user_type=fields_by_type,
        user_type_distribution=dict(dist),
    )
    return snap


def _sample_collection_group_for_schema(
    snapshot_key: str,
    group_name: str,
    order_fields: list[str],
    sample: int = SCHEMA_SUBCOLLECTION_SAMPLE,
) -> dict:
    """Sample recent docs from a collection group (e.g. all users/*/runs)."""
    db = firestore_services.admin_db
    docs_by_path: dict[str, Any] = {}
    for order_field in order_fields:
        docs = _query_recent_docs(
            db.collection_group(group_name),
            coll_label=f"{snapshot_key}({order_field})",
            order_field=order_field,
            sample=sample,
        )
        for doc in docs:
            docs_by_path[doc.reference.path] = doc
    unique_docs = list(docs_by_path.values())
    fields, subcolls, field_counts, subcoll_counts, field_examples, subcoll_examples = (
        _fingerprint_docs(unique_docs)
    )
    return _schema_entry(
        fields,
        subcolls,
        len(unique_docs),
        field_counts=field_counts,
        subcollection_counts=subcoll_counts,
        field_examples=field_examples,
        subcollection_examples=subcoll_examples,
    )


def capture_schema_snapshot() -> dict:
    """Snapshot top-level collections plus stratified users and user subcollections."""
    snap: dict[str, dict] = {}
    for coll, ts in COLLECTION_UPDATED_FIELD.items():
        if coll == "users":
            snap.update(_sample_users_schema_stratified(ts))
        else:
            snap[coll] = _sample_collection_for_schema(coll, ts)
    for key, group, order_fields in USER_SUBCOLLECTION_SCHEMA_SPECS:
        snap[key] = _sample_collection_group_for_schema(key, group, order_fields)
    return snap


def _snapshot_ref(week_iso: str):
    """Doc ref to the per-week snapshot in admin-prod Firestore."""
    return (firestore_services.admin_db
            .collection("logs").document("_weekly_state")
            .collection("snapshots").document(week_iso))


def load_previous_snapshot(prev_week_iso: str) -> dict | None:
    try:
        snap = _snapshot_ref(prev_week_iso).get()
        if not snap.exists:
            return None
        return (snap.to_dict() or {}).get("snapshot")
    except Exception as e:
        logging.warning("weekly_report: failed to load prev snapshot %s: %s",
                        prev_week_iso, e)
        return None


def store_snapshot(week_iso: str, snapshot: dict) -> None:
    try:
        _snapshot_ref(week_iso).set({
            "snapshot": snapshot,
            "captured_at": datetime.now(timezone.utc),
            "api_version": settings.config.get("VERSION"),
        })
    except Exception as e:
        logging.warning("weekly_report: failed to store snapshot %s: %s",
                        week_iso, e)


def _removal_min_sample(coll_key: str) -> int:
    """Per-userType and subcollection keys use a lower bar (smaller fixed sample)."""
    return 3 if "/" in coll_key else 10


def _examples_for_names(
    examples_map: dict | None, names: list[str]
) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    src = examples_map or {}
    for name in names:
        ids = [i for i in (src.get(name) or []) if i][:SCHEMA_EXAMPLE_IDS]
        if ids:
            out[name] = ids
    return out


def detect_schema_drift(current: dict, previous: dict | None) -> dict:
    """Diff current snapshot vs previous. Returns additions/removals.
    A removal is reported only for a field/subcollection present in every
    previous sampled document and absent from an adequately sized current
    sample. Sampling can prove that a field exists, but a sparse sample cannot
    reliably prove that an optional field such as surveyResponses.specific was
    removed. Example locators (doc id, or userId for users/* subdocs) come from
    the same sample — no extra Firestore reads.
    """
    empty_examples: dict = {}
    if previous is None:
        return {
            "first_run": True,
            "added": {},
            "removed": {},
            "subcollections_added": {},
            "subcollections_removed": {},
            "added_examples": empty_examples,
            "removed_examples": empty_examples,
            "subcollections_added_examples": empty_examples,
            "subcollections_removed_examples": empty_examples,
        }

    added_fields: dict[str, list[str]] = {}
    removed_fields: dict[str, list[str]] = {}
    subs_added: dict[str, list[str]] = {}
    subs_removed: dict[str, list[str]] = {}
    added_examples: dict[str, dict[str, list[str]]] = {}
    removed_examples: dict[str, dict[str, list[str]]] = {}
    subs_added_examples: dict[str, dict[str, list[str]]] = {}
    subs_removed_examples: dict[str, dict[str, list[str]]] = {}

    all_colls = set(current.keys()) | set(previous.keys())
    for coll in sorted(all_colls):
        cur = current.get(coll) or {}
        prev = previous.get(coll) or {}
        cur_f = set(cur.get("fields") or [])
        prev_f = set(prev.get("fields") or [])
        cur_s = set(cur.get("subcollections") or [])
        prev_s = set(prev.get("subcollections") or [])

        af = sorted(cur_f - prev_f)
        if af:
            added_fields[coll] = af
            ex = _examples_for_names(cur.get("field_examples"), af)
            if ex:
                added_examples[coll] = ex
        min_sample = _removal_min_sample(coll)
        prev_n = prev.get("sample_size") or 0
        cur_n = cur.get("sample_size") or 0
        if prev_n >= min_sample and cur_n >= min_sample:
            prev_field_counts = prev.get("field_counts") or {}
            rf = sorted(
                field for field in prev_f - cur_f
                if prev_field_counts.get(field) == prev_n
            )
            if rf:
                removed_fields[coll] = rf
                ex = _examples_for_names(prev.get("field_examples"), rf)
                if ex:
                    removed_examples[coll] = ex

        sa = sorted(cur_s - prev_s)
        if sa:
            subs_added[coll] = sa
            ex = _examples_for_names(cur.get("subcollection_examples"), sa)
            if ex:
                subs_added_examples[coll] = ex
        if prev_n >= min_sample and cur_n >= min_sample:
            prev_sub_counts = prev.get("subcollection_counts") or {}
            sr = sorted(
                sub for sub in prev_s - cur_s
                if prev_sub_counts.get(sub) == prev_n
            )
            if sr:
                subs_removed[coll] = sr
                ex = _examples_for_names(prev.get("subcollection_examples"), sr)
                if ex:
                    subs_removed_examples[coll] = ex

    return {
        "first_run": False,
        "added": added_fields,
        "removed": removed_fields,
        "subcollections_added": subs_added,
        "subcollections_removed": subs_removed,
        "added_examples": added_examples,
        "removed_examples": removed_examples,
        "subcollections_added_examples": subs_added_examples,
        "subcollections_removed_examples": subs_removed_examples,
    }


# ----------------------------------------------------------------------------
# Slack format
# ----------------------------------------------------------------------------

def _fmt_int(n) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _fmt_drift_names(names: list[str], examples: dict[str, list[str]] | None) -> str:
    """`field` (`id1`, `id2`) — ids are district/user locators from the sample."""
    ex = examples or {}
    parts = []
    for name in names:
        ids = ex.get(name) or []
        if ids:
            id_text = ", ".join(f"`{i}`" for i in ids)
            parts.append(f"`{name}` ({id_text})")
        else:
            parts.append(f"`{name}`")
    return ", ".join(parts)


def format_slack_message(
    *,
    window_start_pst: datetime,
    window_end_pst: datetime,
    activity: dict,
    redivis: dict,
    scheduler: dict,
    crashes: dict,
    validation: dict,
    drift: dict,
    administrations: dict | None = None,
) -> str:
    lines: list[str] = []
    lines.append(
        f"*LEVANTE weekly report* — "
        f"{window_start_pst.strftime('%a %b %d')} → "
        f"{window_end_pst.strftime('%a %b %d %Y')} (PST)"
    )
    totals = activity.get("totals") or {}
    lines.append(
        f"*Totals*  children {_fmt_int(totals.get('children', 0))} · "
        f"teachers {_fmt_int(totals.get('teachers', 0))} · "
        f"caregivers {_fmt_int(totals.get('caregivers', 0))} · "
        f"runs {_fmt_int(totals.get('runs', 0))} · "
        f"trials {_fmt_int(totals.get('trials', 0))} · "
        f"invalid {_fmt_int(totals.get('invalid', 0))}"
    )
    lines.append(
        f"*Surveys*  children {_fmt_int(totals.get('surveys_children', 0))} · "
        f"teachers {_fmt_int(totals.get('surveys_teachers', 0))} · "
        f"caregivers {_fmt_int(totals.get('surveys_caregivers', 0))}"
    )
    admin = administrations or {}
    admin_items = admin.get("items") or []
    admin_count = admin.get("count", len(admin_items))
    shown = [it.get("name") or "(no name)" for it in admin_items[:5]]
    if admin_count:
        extra = (
            f" · _…and {admin_count - 5} more_" if admin_count > 5 else ""
        )
        names = ", ".join(f"`{n}`" for n in shown) if shown else "_unnamed_"
        lines.append(
            f"*New administrations*  {admin_count} added/opened this week: "
            f"{names}{extra}"
        )
    else:
        lines.append("*New administrations*  none added/opened this week")

    # -- Per-site table (sorted by total activity desc) --
    per_site = activity.get("per_site") or {}
    def site_activity(p):
        if not isinstance(p, dict):
            return 0
        return (
            p.get("users", 0)
            + p.get("runs", 0)
            + p.get("trials", 0)
            + p.get("surveys", 0)
        )
    ranked = sorted(per_site.items(), key=lambda kv: -site_activity(kv[1]))

    zero_sites = [
        k for k, v in ranked
        if site_activity(v) == 0 and not v.get("note")
    ]
    missing_log_sites = [
        k for k, v in ranked
        if site_activity(v) == 0
        and v.get("note") in {"no_logs_in_window", "no_logs_in_or_before_window"}
    ]
    active = [(k, v) for k, v in ranked if site_activity(v) > 0]

    lines.append("")
    lines.append(
        f"*Per-site activity* ({len(active)} active · {len(zero_sites)} quiet · "
        f"{len(missing_log_sites)} missing logs)"
    )
    if not active:
        lines.append("    _no site had measurable activity this week_")
    else:
        for ds, p in active[:30]:
            note = f"  _{p['note']}_" if p.get("note") else ""
            if p.get("has_user_roles"):
                user_s = (
                    f"ch {_fmt_int(p.get('children', 0))} · "
                    f"te {_fmt_int(p.get('teachers', 0))} · "
                    f"cg {_fmt_int(p.get('caregivers', 0))}"
                )
            else:
                user_s = f"users {_fmt_int(p.get('users', 0))}"
            lines.append(
                f"    `{ds:42s}`  {user_s} · "
                f"runs {_fmt_int(p.get('runs', 0))} · "
                f"trials {_fmt_int(p.get('trials', 0))} · "
                f"sv ch {_fmt_int(p.get('surveys_children', 0))}/"
                f"te {_fmt_int(p.get('surveys_teachers', 0))}/"
                f"cg {_fmt_int(p.get('surveys_caregivers', 0))}{note}"
            )
        if len(active) > 30:
            lines.append(f"    _…and {len(active) - 30} more active sites_")

    # -- Quiet sites are distinct from missing validator runs. --
    if zero_sites:
        lines.append("")
        lines.append(f"*Quiet sites (validator ran, no new records)* ({len(zero_sites)})")
        for ds in zero_sites:
            lines.append(f"    • {ds}")
    if missing_log_sites:
        lines.append("")
        lines.append(f"*Sites missing validator logs this week* ({len(missing_log_sites)})")
        for ds in missing_log_sites:
            p = per_site[ds]
            raw_name = p.get("log_dataset_name") or ds
            lines.append(f"    • {ds}  _checked logs/{raw_name}: {p.get('note')}_")

    # -- Redivis --
    rd = redivis or {}
    awaiting = rd.get("awaiting") or []
    versions = rd.get("versions") or {}
    released_with_version = [
        f"{ds}@{(st or {}).get('version_tag')}"
        for ds, st in versions.items()
        if st and st.get("is_released")
    ]
    lines.append("")
    lines.append(
        f"*Redivis state*  released sites: {len(released_with_version)}  ·  "
        f"awaiting/unreleased: {len(awaiting)}"
    )
    if awaiting:
        lines.append("    awaiting:  " + ", ".join(f"`{a}`" for a in sorted(awaiting)))

    # -- Validation health --
    inv = validation.get("invalid_by_site") or {}
    top_invalid = sorted(inv.items(), key=lambda kv: -kv[1])[:5]
    lines.append("")
    lines.append("*Validation health*  "
                 f"log docs scanned this week: {validation.get('log_docs_scanned', 0)}")
    api_versions = validation.get("api_versions") or {}
    if api_versions:
        versions_text = ", ".join(
            f"`{version}`×{count}"
            for version, count in sorted(
                api_versions.items(), key=lambda item: (-item[1], item[0])
            )
        )
        lines.append(f"    validator versions: {versions_text}")
    if any(v for _, v in top_invalid):
        for ds, n in top_invalid:
            if n:
                lines.append(f"    `{ds:42s}`  invalid_data_count = {n}")
    else:
        lines.append("    _no invalid_data_count > 0 in any site_")
    new_schemas = validation.get("new_schemas") or {}
    if any(new_schemas.get(k) for k in ("runs", "trials", "surveys")):
        lines.append("    *new_schemas observed:*")
        for kind in ("runs", "trials", "surveys"):
            items = new_schemas.get(kind) or {}
            if items:
                top_items = sorted(items.items(), key=lambda kv: -kv[1])[:8]
                lines.append(
                    f"      • {kind}: "
                    + ", ".join(f"`{it}`×{c}" for it, c in top_items)
                )

    # -- Scheduler health --
    sh = scheduler or {}
    if sh.get("error"):
        lines.append("")
        lines.append(f"*Scheduler health*  _query failed: {sh['error']}_")
    else:
        by_job = sh.get("by_job") or {}
        if by_job:
            lines.append("")
            lines.append("*Scheduler failures this week*")
            for job, info in sorted(by_job.items(), key=lambda kv: -kv[1]["count"]):
                kinds = info.get("kinds") or {}
                kinds_str = ", ".join(f"{k}×{v}" for k, v in kinds.items())
                lines.append(f"    `{job:50s}`  {info['count']}  ({kinds_str})")
        else:
            lines.append("")
            lines.append("*Scheduler health*  no failures")

    # -- Crash alerts --
    ca = crashes or {}
    lines.append("")
    if ca.get("error"):
        lines.append(f"*Function crashes*  _query failed: {ca['error']}_")
    elif ca.get("count", 0) > 0:
        lines.append(f"*Function crashes*  {ca['count']} caught by top-level handler")
        for s in ca.get("samples") or []:
            lines.append(f"      • {s}")
    else:
        lines.append("*Function crashes*  none")

    # -- Schema drift --
    d = drift or {}
    lines.append("")
    if d.get("first_run"):
        lines.append("*Firebase schema drift*  _baseline snapshot captured; first-week comparison_")
    else:
        added = d.get("added") or {}
        removed = d.get("removed") or {}
        subs_added = d.get("subcollections_added") or {}
        subs_removed = d.get("subcollections_removed") or {}
        any_drift = added or removed or subs_added or subs_removed
        if not any_drift:
            lines.append("*Firebase schema drift*  no detectable changes since last week")
        else:
            lines.append("*Firebase schema drift*")
            added_ex = d.get("added_examples") or {}
            removed_ex = d.get("removed_examples") or {}
            subs_added_ex = d.get("subcollections_added_examples") or {}
            subs_removed_ex = d.get("subcollections_removed_examples") or {}
            for coll, fields in added.items():
                lines.append(
                    f"    + `{coll}` newly observed fields: "
                    + _fmt_drift_names(fields, added_ex.get(coll))
                )
            for coll, fields in removed.items():
                lines.append(
                    f"    - `{coll}` confirmed removed fields: "
                    + _fmt_drift_names(fields, removed_ex.get(coll))
                )
            for coll, subs in subs_added.items():
                lines.append(
                    f"    + `{coll}` newly observed subcollections: "
                    + _fmt_drift_names(subs, subs_added_ex.get(coll))
                )
            for coll, subs in subs_removed.items():
                lines.append(
                    f"    - `{coll}` confirmed removed subcollections: "
                    + _fmt_drift_names(subs, subs_removed_ex.get(coll))
                )

    return "\n".join(lines)


# ----------------------------------------------------------------------------
# Orchestrator
# ----------------------------------------------------------------------------

def run_weekly_report(dry_run: bool = False) -> dict:
    """Main entry point. If ``dry_run`` is true, no Slack post and no snapshot
    is stored (still computes drift against existing prior snapshot)."""
    t0 = time.time()
    start_utc, end_utc, week_iso = calendar_week_window_pst()
    start_pst = start_utc.astimezone(PST)
    end_pst = end_utc.astimezone(PST)
    prev_week_iso = (datetime.fromisoformat(start_pst.isoformat()) - timedelta(days=7)).strftime("%G-W%V")

    logging.info(
        "weekly_report: window %s → %s PST (week=%s, prev=%s, dry_run=%s)",
        start_pst.isoformat(), end_pst.isoformat(), week_iso, prev_week_iso, dry_run,
    )

    sites = list_active_sites()
    logging.info("weekly_report: %s active sites with siteId", len(sites))

    activity = collect_firestore_activity(sites, start_utc, end_utc)
    administrations = collect_new_administrations(sites, start_utc, end_utc)
    redivis = collect_redivis_state(sites)
    scheduler = collect_scheduler_health(start_utc, end_utc)
    crashes = collect_crash_alerts(start_utc, end_utc)
    validation = collect_validation_health(sites, start_utc, end_utc)

    current_snapshot = capture_schema_snapshot()
    previous_snapshot = load_previous_snapshot(prev_week_iso)
    drift = detect_schema_drift(current_snapshot, previous_snapshot)
    if not dry_run:
        store_snapshot(week_iso, current_snapshot)

    message = format_slack_message(
        window_start_pst=start_pst,
        window_end_pst=end_pst,
        activity=activity,
        redivis=redivis,
        scheduler=scheduler,
        crashes=crashes,
        validation=validation,
        drift=drift,
        administrations=administrations,
    )

    webhook_secret = (settings.config.get(
        "SLACK_WEEKLY_REPORT_WEBHOOK_SECRET_ID") or "").strip()
    if not webhook_secret:
        webhook_secret = settings.config["SLACK_NOTIFICATION_WEB_HOOK"]

    if not dry_run:
        try:
            notify_slack(message, secret_id=webhook_secret)
            slack_posted = True
            slack_error = None
        except Exception as e:
            logging.error("weekly_report: slack post failed: %s", e)
            slack_posted = False
            slack_error = str(e)[:200]
    else:
        slack_posted = False
        slack_error = "dry_run=true"

    return {
        "window_start_pst": start_pst.isoformat(),
        "window_end_pst": end_pst.isoformat(),
        "week_iso": week_iso,
        "previous_week_iso": prev_week_iso,
        "dry_run": dry_run,
        "slack_posted": slack_posted,
        "slack_webhook_secret_id": webhook_secret,
        "slack_error": slack_error,
        "elapsed_sec": round(time.time() - t0, 2),
        "site_count": len(sites),
        "activity": activity,
        "administrations": {
            "count": administrations.get("count", 0),
            "names": [it.get("name") for it in (administrations.get("items") or [])],
        },
        "redivis": redivis,
        "scheduler": scheduler,
        "crashes": crashes,
        "validation": validation,
        "drift_summary": {
            "first_run": drift.get("first_run"),
            "fields_added": {k: len(v) for k, v in (drift.get("added") or {}).items()},
            "fields_removed": {k: len(v) for k, v in (drift.get("removed") or {}).items()},
            "subcollections_added": drift.get("subcollections_added") or {},
            "subcollections_removed": drift.get("subcollections_removed") or {},
        },
        "message_preview": message,
    }
