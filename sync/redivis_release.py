import logging
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import settings
from shared.airtable_services import AirtableServices
from shared.firestore_services import firestore_services
from shared.slack_services import notify_slack
from sync.open_assignments import _airtable_checkbox_truthy
from validators.redivis_services import RedivisServices

logging.basicConfig(level=logging.INFO)


_SAFE_JOB_ID = re.compile(r"[^a-zA-Z0-9_-]")


def _safe_job_id_suffix(suffix: str) -> str:
    """Match shared.scheduler_services._safe_job_id without importing the client."""
    cleaned = _SAFE_JOB_ID.sub("-", suffix.strip()) if suffix else "unnamed"
    cleaned = cleaned.strip("-_") or "unnamed"
    return cleaned[:500]


def _build_dry_run_scheduler_plan(*, dataset_id: str, payload: dict) -> dict:
    """
    Snapshot of the Cloud Scheduler job that *would* be created in a live run.
    Computed locally so dry_run never needs to import the Scheduler client.
    """
    from shared.run_job_services import run_job_api_uri
    from shared.scheduler_services import compute_staggered_cron

    project_id = os.getenv("project_id") or "{project_id}"
    region = settings.config.get("CLOUD_SCHEDULER_REGION", "us-central1")
    timezone = settings.config.get("CLOUD_SCHEDULER_TIMEZONE", "America/Los_Angeles")
    cron = compute_staggered_cron(dataset_id)
    prefix = (settings.config.get("CLOUD_SCHEDULER_JOB_PREFIX") or "").strip("-")
    url = run_job_api_uri(project_id=project_id, region=region)
    suffix = _safe_job_id_suffix(dataset_id)
    job_id = f"{prefix}-{suffix}" if prefix else suffix
    retry_config = {
        "retry_count": int(
            settings.config.get("CLOUD_SCHEDULER_RETRY_COUNT", 3)
        ),
        "max_retry_duration_seconds": int(
            settings.config.get("CLOUD_SCHEDULER_RETRY_MAX_DURATION_SECONDS", 1800)
        ),
        "min_backoff_seconds": int(
            settings.config.get("CLOUD_SCHEDULER_RETRY_MIN_BACKOFF_SECONDS", 60)
        ),
        "max_backoff_seconds": int(
            settings.config.get("CLOUD_SCHEDULER_RETRY_MAX_BACKOFF_SECONDS", 600)
        ),
        "max_doublings": int(
            settings.config.get("CLOUD_SCHEDULER_RETRY_MAX_DOUBLINGS", 3)
        ),
    }
    attempt_deadline_seconds = int(
        settings.config.get("CLOUD_SCHEDULER_ATTEMPT_DEADLINE_SECONDS", 1800)
    )
    return {
        "project_id": project_id,
        "region": region,
        "job_id": job_id,
        "job_full_name": f"projects/{project_id}/locations/{region}/jobs/{job_id}",
        "schedule": cron,
        "timezone": timezone,
        "method": "POST",
        "url": url,
        "headers": ["Content-Type"],
        "auth": "OAuth (Cloud Scheduler service account → Run Job API)",
        "payload": payload,
        "description": f"Pushing {dataset_id} data to redivis on daily basis",
        "retry_config": retry_config,
        "attempt_deadline_seconds": attempt_deadline_seconds,
    }


def _slack_secret_redivis(*, dry_run: bool) -> str:
    if dry_run:
        admin = (settings.config.get("SLACK_ADMIN_WEBHOOK_SECRET_ID") or "").strip()
        if admin:
            return admin
    return settings.config["SLACK_NOTIFICATION_WEB_HOOK"]


def _today_in_pdt() -> str:
    """YYYY-MM-DD string in America/Los_Angeles for Airtable date columns."""
    return datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y-%m-%d")


def _airtable_date_is_empty(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


# Suffix appended to the Airtable Name to form the raw dataset on Redivis.
# Airtable Name = processed (unmarked); Redivis name / scheduler target = {Name}-raw.
RAW_DATASET_SUFFIX = settings.config.get("RAW_DATASET_SUFFIX", "-raw")


def _processed_dataset_name(dataset_id: str) -> str:
    """Processed dataset is the unmarked Airtable Name."""
    return dataset_id


def _raw_dataset_name(dataset_id: str) -> str:
    """Raw dataset written by the validator cron: ``{Name}-raw``."""
    return f"{dataset_id}{RAW_DATASET_SUFFIX}"


def _airtable_text_is_empty(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _ensure_redivis_dataset(
    rs: "RedivisServices",
    dataset_id: str,
    *,
    dry_run: bool,
    changes: list,
    notes: list,
    counters: dict,
) -> tuple[bool, str | None]:
    """
    Idempotently ensure a Redivis dataset exists.

    Returns ``(ok, reference_id)``. ``ok`` is False only on a live create/get
    failure. ``reference_id`` is set when the dataset exists (or was just created).
    """
    rs.set_dataset(dataset_id=dataset_id)
    if dry_run:
        pre_status = rs.get_current_dataset_status()
        if pre_status.get("exists"):
            logging.info(
                "redivis_individual_release: [dry_run] Redivis dataset %r "
                "already exists — would skip",
                dataset_id,
            )
            return True, rs.get_reference_id()
        changes.append(
            f"[dry_run] would create empty Redivis dataset `{dataset_id}`"
        )
        logging.info(
            "redivis_individual_release: [dry_run] would create empty "
            "Redivis dataset %r",
            dataset_id,
        )
        return True, None

    create_status = rs.create_empty_dataset_if_missing()
    if create_status.get("created"):
        counters["redivis_datasets_created"] += 1
        changes.append(f"Created empty Redivis dataset `{dataset_id}`")
    elif create_status.get("already_exists"):
        counters["redivis_datasets_already_existed"] += 1
        logging.info(
            "redivis_individual_release: Redivis dataset %r already exists — skipped",
            dataset_id,
        )
    if create_status.get("error"):
        counters["redivis_datasets_failed"] += 1
        changes.append(
            f"Redivis dataset `{dataset_id}` create error: {create_status['error']}"
        )
        return False, None
    return True, rs.get_reference_id()


def _build_validator_payload(*, dataset_id: str, site_id: str) -> dict:
    """Static template that matches the daily-cron contract documented in README.md."""
    return {
        "dataset_id": dataset_id,
        "is_save_to_storage": True,
        "is_force_uploading_to_redivis": False,
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


def _resolve_site_id_from_names(
    *, name: str | None, firebase_name: str | None
) -> tuple[str | None, str | None]:
    """
    Try ``Name`` → ``Firebase name`` against ``districts.name`` /
    ``districts.normalizedName``. Returns ``(site_id, source_field_label)`` or
    ``(None, None)`` when no Firestore district matches. Does not use
    ``Redivis name`` (that value is ``{Name}-raw`` and is not a Firebase district).
    """
    candidates = [
        ("Name", name),
        ("Firebase name", firebase_name),
    ]
    for label, raw in candidates:
        s = (raw or "").strip() if raw is not None else ""
        if not s:
            continue
        looked = firestore_services.find_district_id_by_name(s)
        if looked:
            return looked, label
    return None, None


def _format_slack(
    *,
    dry_run: bool,
    rows: list[dict],
    awaiting: list[dict],
    fetch_diagnostics: dict | None = None,
) -> str:
    """
    Single Slack summary covering both 'awaiting release' info and per-row Airtable
    changes (siteId resolution, validator pipeline date, scheduler creation,
    empty-dataset creation). In live runs, only rows with at least one change
    are listed.
    """
    mode = "*[DRY RUN — read-only, no Airtable / Cloud Scheduler writes]* " if dry_run else ""
    lines = [
        f"{mode}*Redivis individual ↔ datasets sync*",
        "",
    ]

    if fetch_diagnostics:
        diag_parts = [
            f"raw fetched {fetch_diagnostics.get('raw_fetched', 0)}",
            f"after {settings.config['AIRTABLE_FIELD_REDIVIS_INDIVIDUAL']!r} check"
            f" {fetch_diagnostics.get('kept_after_individual_check', 0)}"
            f" (dropped {fetch_diagnostics.get('dropped_no_individual', 0)})",
        ]
        if fetch_diagnostics.get("single_dataset_filter_applied"):
            diag_parts.append(
                f"after single-dataset filter "
                f"{fetch_diagnostics.get('single_dataset_kept', 0)}"
            )
        lines.append(f"_Airtable rows: {' · '.join(diag_parts)}_")
        lines.append("")

    if not rows:
        if fetch_diagnostics and fetch_diagnostics.get("raw_fetched", 0) == 0:
            lines.append(
                f"_Airtable returned 0 rows for base "
                f"`{settings.config.get('AIRTABLE_LEVANTE_ENTITIES_BASE_ID')}` "
                f"table `{settings.config.get('AIRTABLE_DATASET_TABLE_ID')}`._"
            )
        else:
            lines.append(
                f"_No Airtable rows with *{settings.config['AIRTABLE_FIELD_REDIVIS_INDIVIDUAL']}* checked"
                f" (after filter)._"
            )
        return "\n".join(lines)

    if dry_run:
        rows_to_show = rows
    else:
        # Show rows that either had an Airtable change, an informational note
        # (e.g. scheduler job already exists), or are part of a single-dataset
        # manual invocation (signaled by single_dataset_filter_applied).
        single_dataset = bool(
            fetch_diagnostics
            and fetch_diagnostics.get("single_dataset_filter_applied")
        )
        if single_dataset:
            rows_to_show = rows
        else:
            rows_to_show = [r for r in rows if r.get("changes") or r.get("notes")]
        if not rows_to_show and not awaiting:
            lines.append("_No Airtable changes and no sites awaiting release._")
            return "\n".join(lines)

    if dry_run:
        lines.append(f"*Per-site status ({len(rows_to_show)})*")
    elif rows_to_show:
        lines.append(f"*Sites ({len(rows_to_show)})*")

    for row in rows_to_show:
        rid = row["airtable_record_id"]
        label = row["site_label"]
        ds = row.get("redivis_dataset_name") or "—"
        site_id = row.get("site_id") or "—"
        ex = row.get("dataset_exists")
        rel = row.get("is_released")
        tag = row.get("version_tag")
        deleted = row.get("is_deleted")

        ex_s = "yes" if ex else ("n/a" if ex is None else "no")
        rel_s = "yes" if rel else ("n/a" if not ex else "no")
        tag_s = f"`{tag}`" if tag else "—"
        del_s = f" deleted={deleted}" if deleted is not None else ""

        lines.append(
            f"\n• *{label}* — Airtable `{rid}` — dataset `{ds}` — siteId `{site_id}`"
        )
        lines.append(
            f"    on Redivis: *{ex_s}* — released: *{rel_s}* — version {tag_s}{del_s}"
        )

        changes = row.get("changes") or []
        if changes:
            lines.append("    *Changes:*")
            for c in changes:
                lines.append(f"      ◦ {c}")

        row_notes = row.get("notes") or []
        if row_notes:
            lines.append("    *Notes:*")
            for n in row_notes:
                lines.append(f"      ◦ {n}")

        if dry_run:
            sched = row.get("scheduler_status") or {}
            plan = sched.get("plan") if isinstance(sched, dict) else None
            if plan:
                payload = plan.get("payload") or {}
                org_filter_value = (
                    payload.get("orgs", [{}])[0]
                    .get("filters", {})
                    .get("org_filter", {})
                    .get("value", [])
                )
                lines.append("    *Cloud Scheduler (planned):*")
                lines.append(f"      ◦ Job: `{plan['job_id']}`")
                lines.append(
                    f"      ◦ Schedule: `{plan['schedule']}` ({plan['timezone']})"
                )
                lines.append(
                    f"      ◦ Target: `{plan['method']} {plan['url']}` "
                    f"(attempt deadline {plan.get('attempt_deadline_seconds', 180)}s)"
                )
                lines.append(
                    f"      ◦ Headers: {', '.join(f'`{h}`' for h in plan['headers'])}"
                )
                lines.append(
                    "      ◦ Payload: "
                    f"`dataset_id={payload.get('dataset_id')}`, "
                    f"`org_id={payload.get('orgs', [{}])[0].get('org_id')}`, "
                    f"`org_filter.value={org_filter_value}`"
                )
                retry_cfg = plan.get("retry_config") or {}
                if retry_cfg:
                    lines.append(
                        "      ◦ Retry: "
                        f"`count={retry_cfg.get('retry_count')}` · "
                        f"`max_duration={retry_cfg.get('max_retry_duration_seconds')}s` · "
                        f"`backoff={retry_cfg.get('min_backoff_seconds')}-"
                        f"{retry_cfg.get('max_backoff_seconds')}s` · "
                        f"`doublings={retry_cfg.get('max_doublings')}`"
                    )
            elif (
                row.get("redivis_dataset_name")
                and row.get("site_id")
                and row.get("site_id") != settings.config["MISSING_SITE_ID_PLACEHOLDER"]
            ):
                lines.append(
                    "    *Cloud Scheduler (planned):* _no plan computed (see logs)_"
                )

    if not dry_run and awaiting:
        lines.append("")
        lines.append(f"*Sites awaiting Redivis individual release ({len(awaiting)})*")
        for row in awaiting:
            lines.append(
                f"• {row['site_label']} — dataset `{row['redivis_dataset_name']}`"
                f" (Airtable `{row['airtable_record_id']}`)"
            )

    return "\n".join(lines)


def check_redivis_individual_release_awaiting_slack(
    *, dry_run: bool = False, dataset_name: str | None = None
) -> dict:
    """
    For every Airtable row with **Redivis individual** checked:

    Naming convention (post ``-raw`` migration):
      - Airtable **Name** = processed Redivis dataset (unmarked)
      - Airtable **Redivis name** = raw Redivis dataset (``{Name}-raw``)
      - Validator cron / GCS / Redivis upload target = raw (``{Name}-raw``)

    1. If **Firestore siteId** is empty, try to resolve it from ``Name`` →
       ``Firebase name`` against Firestore ``districts`` (matches ``name`` or
       ``normalizedName``). When found, write the id back to Airtable; when not
       found, write the placeholder ``missing_site_id``.
    2. Backfill **Redivis name** to ``{Name}-raw`` when the cell is empty (never
       overwrite an existing value).
    3. When a real siteId exists, ensure empty Redivis datasets exist first:
       ``{Name}-raw`` (raw upload target) and unmarked ``{Name}`` (processed
       companion). Idempotent create-if-missing.
    4. Backfill **dataset_ref_id** (raw) and **processed_ref_id** (processed)
       from Redivis ``referenceId`` only when those Airtable cells are empty.
    5. Only after both Redivis datasets are in place, create a daily Cloud
       Scheduler job (staggered ~12:00 PDT) targeting the **raw** dataset id.
       **Create-if-missing only** — an existing job is left unchanged. On
       success, today's date (PDT) is written to **validator pipeline date**
       if that cell is empty.

    Single-dataset mode: pass ``dataset_name`` to limit processing to the one
    Airtable row whose **Name** matches (case-insensitive, trimmed). When no row
    matches, the function returns normally with an empty result instead of raising.

    Slack: dry-run always posts a full summary; live runs post only when at least
    one Airtable cell was updated for some row, or there are sites still awaiting
    release; single-dataset mode always posts so manual invocations get feedback.

    **Dry run** is fully read-only: no Airtable writes, no Cloud Scheduler writes,
    no Redivis dataset creation; Firestore / Redivis lookups still execute (they
    are read-only API usage).
    """
    individual_field = settings.config["AIRTABLE_FIELD_REDIVIS_INDIVIDUAL"]
    name_field = settings.config["AIRTABLE_FIELD_REDIVIS_DATASET_NAME"]
    site_field = settings.config["AIRTABLE_FIELD_FIRESTORE_SITE_ID"]
    firebase_name_field = settings.config["AIRTABLE_FIELD_SITE_NAME"]
    redivis_name_field = settings.config["AIRTABLE_FIELD_REDIVIS_NAME"]
    pipeline_date_field = settings.config["AIRTABLE_FIELD_VALIDATOR_PIPELINE_DATE"]
    dataset_ref_field = settings.config["AIRTABLE_FIELD_DATASET_REF_ID"]
    processed_ref_field = settings.config["AIRTABLE_FIELD_PROCESSED_REF_ID"]
    missing_placeholder = settings.config["MISSING_SITE_ID_PLACEHOLDER"]

    target_name = (dataset_name or "").strip()
    target_name_lower = target_name.lower() or None

    logging.info(
        "redivis_individual_release: starting dry_run=%s dataset_name=%r base=%s table=%s",
        dry_run,
        target_name or None,
        settings.config.get("AIRTABLE_LEVANTE_ENTITIES_BASE_ID"),
        settings.config.get("AIRTABLE_DATASET_TABLE_ID"),
    )

    airtable = AirtableServices()
    rs = RedivisServices()
    scheduler = None  # lazily created so dry_run never imports a client
    scheduler_unavailable_reason: str | None = None

    # Prefer filterByFormula on the Redivis individual checkbox; fall back to a
    # full fetch + Python-side truthy check if the formula query fails.
    formula = f"{{{individual_field}}}=1"
    try:
        raw_records = airtable.list_dataset_records(formula=formula)
        logging.info(
            "redivis_individual_release: filterByFormula returned %s rows (formula=%r)",
            len(raw_records),
            formula,
        )
    except Exception as e:
        logging.warning(
            "redivis_individual_release: filterByFormula failed (%s); fetching all rows",
            e,
        )
        raw_records = airtable.list_dataset_records()
        logging.info(
            "redivis_individual_release: fetched %s total Airtable rows (no filter)",
            len(raw_records),
        )

    rows_dropped_no_individual = 0
    enforced_records: list[dict] = []
    for rec in raw_records:
        fields = rec.get("fields") or {}
        raw_flag = fields.get(individual_field)
        if _airtable_checkbox_truthy(raw_flag):
            enforced_records.append(rec)
        else:
            rows_dropped_no_individual += 1
            logging.debug(
                "redivis_individual_release: skip record=%s (no/false %r value=%r type=%s)",
                rec.get("id"),
                individual_field,
                raw_flag,
                type(raw_flag).__name__,
            )
    if rows_dropped_no_individual:
        logging.info(
            "redivis_individual_release: dropped %s row(s) without %r checked",
            rows_dropped_no_individual,
            individual_field,
        )
    records = enforced_records

    fetch_diagnostics = {
        "raw_fetched": len(raw_records),
        "kept_after_individual_check": len(records),
        "dropped_no_individual": rows_dropped_no_individual,
        "single_dataset_filter_applied": False,
        "single_dataset_kept": None,
    }

    if target_name_lower is not None:
        filtered = [
            rec
            for rec in records
            if str(((rec.get("fields") or {}).get(name_field) or "")).strip().lower()
            == target_name_lower
        ]
        if len(filtered) > 1:
            logging.warning(
                "redivis_individual_release: %s rows matched %s=%r; processing all",
                len(filtered),
                name_field,
                target_name,
            )
        records = filtered
        fetch_diagnostics["single_dataset_filter_applied"] = True
        fetch_diagnostics["single_dataset_kept"] = len(records)
        if not records:
            logging.warning(
                "redivis_individual_release: single-dataset mode found 0 rows for "
                "%s=%r AND %r checked",
                name_field,
                target_name,
                individual_field,
            )
        else:
            logging.info(
                "redivis_individual_release: single-dataset mode kept %s row(s)",
                len(records),
            )

    rows_summary: list[dict] = []
    awaiting: list[dict] = []
    today = _today_in_pdt()

    counters = {
        "rows_with_individual_flag": 0,
        "site_id_resolved_from_names": 0,
        "site_id_set_missing_placeholder": 0,
        "redivis_name_backfilled": 0,
        "scheduler_jobs_created": 0,
        "scheduler_jobs_already_existed": 0,
        "scheduler_jobs_failed": 0,
        "redivis_datasets_created": 0,
        "redivis_datasets_already_existed": 0,
        "redivis_datasets_failed": 0,
        "pipeline_date_written": 0,
        "dataset_ref_id_written": 0,
        "processed_ref_id_written": 0,
    }

    for rec in records:
        rid = rec["id"]
        fields = rec.get("fields") or {}
        if not fields.get(individual_field):
            continue

        counters["rows_with_individual_flag"] += 1

        name_raw = fields.get(name_field)
        dataset_name = str(name_raw).strip() if name_raw is not None else ""
        site_label = dataset_name or "(no name)"
        processed_name = _processed_dataset_name(dataset_name) if dataset_name else ""
        raw_name = _raw_dataset_name(dataset_name) if dataset_name else ""

        site_raw = fields.get(site_field)
        site_id = str(site_raw).strip() if site_raw is not None else ""

        airtable_payload: dict = {}
        changes: list[str] = []
        notes: list[str] = []

        # 1) Resolve Firestore siteId if empty.
        site_id_source = "existing"
        if not site_id:
            resolved, source_label = _resolve_site_id_from_names(
                name=fields.get(name_field),
                firebase_name=fields.get(firebase_name_field),
            )
            if resolved:
                site_id = resolved
                site_id_source = source_label or "lookup"
                airtable_payload[site_field] = site_id
                counters["site_id_resolved_from_names"] += 1
                changes.append(
                    f"Set `{site_field}` → `{site_id}` (matched on *{site_id_source}*)"
                )
            else:
                site_id = missing_placeholder
                site_id_source = "missing"
                airtable_payload[site_field] = site_id
                counters["site_id_set_missing_placeholder"] += 1
                changes.append(
                    f"Set `{site_field}` → `{missing_placeholder}` "
                    "(no Firestore district match for Name / Firebase name)"
                )

        # 1.5) Backfill Redivis name → {Name}-raw only when empty.
        if dataset_name and _airtable_text_is_empty(fields.get(redivis_name_field)):
            airtable_payload[redivis_name_field] = raw_name
            counters["redivis_name_backfilled"] += 1
            changes.append(
                f"Set `{redivis_name_field}` → `{raw_name}`"
                + (" [dry_run]" if dry_run else "")
            )

        # 2) Redivis shells first (before cron): {Name}-raw + unmarked {Name}.
        # Then ref-id backfill; then create-if-missing scheduler job.
        scheduler_status: dict | None = None
        if dataset_name and site_id and site_id != missing_placeholder:
            raw_ok, raw_ref_id = _ensure_redivis_dataset(
                rs,
                raw_name,
                dry_run=dry_run,
                changes=changes,
                notes=notes,
                counters=counters,
            )
            processed_ok, processed_ref_id = _ensure_redivis_dataset(
                rs,
                processed_name,
                dry_run=dry_run,
                changes=changes,
                notes=notes,
                counters=counters,
            )

            if raw_ref_id and _airtable_text_is_empty(fields.get(dataset_ref_field)):
                airtable_payload[dataset_ref_field] = raw_ref_id
                counters["dataset_ref_id_written"] += 1
                changes.append(
                    f"Set `{dataset_ref_field}` → `{raw_ref_id}` (raw)"
                    + (" [dry_run]" if dry_run else "")
                )
            if processed_ref_id and _airtable_text_is_empty(
                fields.get(processed_ref_field)
            ):
                airtable_payload[processed_ref_field] = processed_ref_id
                counters["processed_ref_id_written"] += 1
                changes.append(
                    f"Set `{processed_ref_field}` → `{processed_ref_id}` (processed)"
                    + (" [dry_run]" if dry_run else "")
                )

            datasets_ready = raw_ok and processed_ok
            if not datasets_ready:
                changes.append(
                    "Skipped Cloud Scheduler — Redivis raw/processed dataset "
                    "shells not both ready"
                )
            else:
                payload = _build_validator_payload(
                    dataset_id=raw_name, site_id=site_id
                )
                if dry_run:
                    plan = _build_dry_run_scheduler_plan(
                        dataset_id=raw_name, payload=payload
                    )
                    scheduler_status = {
                        "created": False,
                        "already_exists": False,
                        "job_name": plan["job_full_name"],
                        "url": plan["url"],
                        "error": None,
                        "would_create": True,
                        "plan": plan,
                    }
                    logging.info(
                        "redivis_individual_release: [dry_run] would create Cloud Scheduler job "
                        "id=%s schedule=%r tz=%s url=%s payload_dataset_id=%s "
                        "payload_org_filter_value=%s",
                        plan["job_id"],
                        plan["schedule"],
                        plan["timezone"],
                        plan["url"],
                        payload["dataset_id"],
                        payload["orgs"][0]["filters"]["org_filter"]["value"],
                    )
                    changes.append(
                        f"[dry_run] would ensure Cloud Scheduler job `{plan['job_id']}` "
                        f"({plan['schedule']} {plan['timezone']})"
                    )
                else:
                    if scheduler is None:
                        try:
                            from shared.scheduler_services import SchedulerServices
                        except ImportError as e:
                            scheduler_unavailable_reason = (
                                f"google-cloud-scheduler not installed in this "
                                f"deployment ({e}). Redeploy with the updated "
                                f"requirements.txt."
                            )
                            logging.error(
                                "redivis_individual_release: %s",
                                scheduler_unavailable_reason,
                            )
                            scheduler = False
                        else:
                            try:
                                scheduler = SchedulerServices()
                            except Exception as e:
                                scheduler_unavailable_reason = (
                                    f"scheduler_init_error: {e}"
                                )
                                logging.error(
                                    "redivis_individual_release: scheduler init failed: %s",
                                    e,
                                )
                                scheduler = False  # sentinel — don't keep retrying
                    if scheduler is False:
                        scheduler_status = {
                            "created": False,
                            "already_exists": False,
                            "job_name": None,
                            "url": None,
                            "error": scheduler_unavailable_reason
                            or "scheduler_init_failed",
                        }
                        counters["scheduler_jobs_failed"] += 1
                        changes.append(
                            f"Scheduler unavailable: {scheduler_status['error']}"
                        )
                    else:
                        scheduler_status = scheduler.get_or_create_validator_job(
                            dataset_id=raw_name, payload=payload
                        )
                        if scheduler_status.get("created"):
                            counters["scheduler_jobs_created"] += 1
                            changes.append(
                                f"Created Cloud Scheduler job `{scheduler_status['job_name']}`"
                            )
                        elif scheduler_status.get("already_exists"):
                            counters["scheduler_jobs_already_existed"] += 1
                            logging.info(
                                "redivis_individual_release: scheduler job already exists "
                                "for dataset_id=%s name=%s — skipping create",
                                raw_name,
                                scheduler_status.get("job_name"),
                            )
                        if scheduler_status.get("error"):
                            counters["scheduler_jobs_failed"] += 1
                            changes.append(
                                f"Scheduler error: {scheduler_status['error']}"
                            )

                # Backfill validator pipeline date when empty and the job is present.
                job_present = bool(
                    scheduler_status
                    and not scheduler_status.get("error")
                    and (
                        scheduler_status.get("created")
                        or scheduler_status.get("already_exists")
                        or scheduler_status.get("would_create")
                    )
                )
                if job_present and _airtable_date_is_empty(
                    fields.get(pipeline_date_field)
                ):
                    airtable_payload[pipeline_date_field] = today
                    counters["pipeline_date_written"] += 1
                    changes.append(
                        f"Set `{pipeline_date_field}` → `{today}`"
                        + (" [dry_run]" if dry_run else "")
                    )

        # 3) Redivis status — awaiting release tracks the *raw* upload target.
        redivis_status = {
            "exists": None,
            "is_released": None,
            "version_tag": None,
            "is_deleted": None,
        }
        if raw_name:
            rs.set_dataset(dataset_id=raw_name)
            redivis_status = rs.get_current_dataset_status()

            if not (redivis_status.get("exists") and redivis_status.get("is_released")):
                awaiting.append(
                    {
                        "airtable_record_id": rid,
                        "site_label": site_label,
                        "redivis_dataset_name": raw_name,
                    }
                )

        # 4) Apply Airtable writes (live only) — single update per record.
        if airtable_payload and not dry_run:
            try:
                airtable.update_record_fields(rid, airtable_payload)
                logging.info(
                    "redivis_individual_release: updated record=%s fields=%s",
                    rid,
                    sorted(airtable_payload.keys()),
                )
            except Exception as e:
                logging.error(
                    "redivis_individual_release: failed to update record=%s fields=%s: %s",
                    rid,
                    sorted(airtable_payload.keys()),
                    e,
                )
                changes.append(f"Airtable update FAILED: {e}")

        rows_summary.append(
            {
                "airtable_record_id": rid,
                "site_label": site_label,
                "redivis_dataset_name": raw_name or None,
                "processed_dataset_name": processed_name or None,
                "site_id": site_id or None,
                "site_id_source": site_id_source,
                "dataset_exists": redivis_status.get("exists"),
                "is_released": redivis_status.get("is_released"),
                "version_tag": redivis_status.get("version_tag"),
                "is_deleted": redivis_status.get("is_deleted"),
                "scheduler_status": scheduler_status,
                "changes": changes,
                "notes": notes,
            }
        )

    rows_with_changes = sum(1 for r in rows_summary if r.get("changes"))
    logging.info(
        "redivis_individual_release: summary %s rows_with_changes=%s awaiting=%s counters=%s",
        len(rows_summary),
        rows_with_changes,
        len(awaiting),
        counters,
    )

    # Slack: dry_run always; single-dataset mode always (manual invocation deserves
    # feedback even when nothing changed); batch live runs only when at least one
    # row actually had an Airtable change.
    should_slack = (
        dry_run
        or target_name_lower is not None
        or rows_with_changes > 0
    )
    slack_secret = _slack_secret_redivis(dry_run=dry_run)
    if should_slack:
        msg = _format_slack(
            dry_run=dry_run,
            rows=rows_summary,
            awaiting=awaiting,
            fetch_diagnostics=fetch_diagnostics,
        )
        logging.info(
            "redivis_individual_release: sending Slack summary "
            "(webhook_secret_id=%s rows_with_changes=%s)",
            slack_secret,
            rows_with_changes,
        )
        notify_slack(msg, secret_id=slack_secret)
    else:
        logging.info(
            "redivis_individual_release: skipping Slack (no Airtable changes; not dry_run)"
        )

    logging.info("redivis_individual_release: finished dry_run=%s", dry_run)

    return {
        "dry_run": dry_run,
        "airtable_rows_total": len(records),
        "rows_with_changes": rows_with_changes,
        "awaiting_release_count": len(awaiting),
        "counters": counters,
        "fetch_diagnostics": fetch_diagnostics,
        "awaiting": awaiting,
        "rows_summary": rows_summary,
    }
