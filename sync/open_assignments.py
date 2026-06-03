import logging
from datetime import datetime, timezone

import settings
from shared.airtable_services import AirtableServices
from shared.firestore_services import firestore_services
from shared.slack_services import notify_slack

logging.basicConfig(level=logging.INFO)


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
            s = value.replace("Z", "+00:00")
            return datetime.fromisoformat(s)
        except ValueError:
            return None
    return None


def administration_covers_now(
    date_opened, date_closed, now: datetime | None = None
) -> bool:
    """True if `now` falls within [dateOpened, dateClosed] (inclusive on timestamps)."""
    now = now or datetime.now(timezone.utc)
    opened = _to_utc_datetime(date_opened)
    closed = _to_utc_datetime(date_closed)
    if opened is None or closed is None:
        return False
    return opened <= now <= closed


def get_open_administrations_for_district(
    district_id: str, now: datetime | None = None
) -> list[dict]:
    """
    Administrations (assignments) for this district/site whose window includes `now`.
    Each item: administration_id, administration_name (from Firestore `name`).
    """
    out: list[dict] = []
    for admin in firestore_services.iter_administrations_for_site(district_id):
        if administration_covers_now(
            admin.get("dateOpened"),
            admin.get("dateClosed"),
            now=now,
        ):
            aid = admin.get("administration_id")
            out.append(
                {
                    "administration_id": aid,
                    "administration_name": (admin.get("name") or "").strip() or "(no name)",
                }
            )
    return out


def _airtable_checkbox_truthy(value) -> bool:
    """Whether an Airtable checkbox / optional field counts as checked/true."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


def _slack_webhook_secret_id_for_open_assignments(*, dry_run: bool) -> str:
    if dry_run:
        admin = (settings.config.get("SLACK_ADMIN_WEBHOOK_SECRET_ID") or "").strip()
        if admin:
            return admin
    return settings.config["SLACK_NOTIFICATION_WEB_HOOK"]


def _open_assignments_flipped(prev, has_open: bool) -> bool:
    """
    True only when the Airtable Open Assignments value was explicitly false and is now true,
    or was true and is now false. Missing/None prior value is not counted as a flip.
    """
    if prev is None:
        return False
    return bool(prev) != has_open


def _format_open_assignments_slack_summary(
    *,
    dry_run: bool,
    rows_updated: int,
    skipped_no_site_id: int,
    districts_detail: list[dict],
) -> str:
    mode = "*[DRY RUN — no Airtable writes]* " if dry_run else ""
    lines = [
        f"{mode}*Open assignments sync (Airtable)*",
        (
            f"Rows where Open Assignments toggled false ↔ true (dry-run count): {rows_updated}"
            if dry_run
            else f"Rows where Open Assignments toggled false ↔ true: {rows_updated}"
        ),
        f"Rows skipped (no district/site id): {skipped_no_site_id}",
        "",
        "Terminology: *assignment* = Firestore `administrations` doc; *site* = *district* (`districts` collection).",
        "",
        (
            "_Below: all districts with at least one open administration (Firestore), among processed Redivis-individual rows._"
            if dry_run
            else "_Below: open administrations (Firestore) for sites whose Airtable row was updated._"
        ),
        "",
    ]
    if not districts_detail:
        lines.append(
            "_No districts currently have an open assignment._"
            if dry_run
            else "_No sites listed (unexpected if rows were updated)._"
        )
        return "\n".join(lines)

    lines.append(
        (
            f"*Districts with at least one open assignment ({len(districts_detail)}):*"
            if dry_run
            else f"*Sites with an Airtable change — open administrations now ({len(districts_detail)}):*"
        )
    )
    for row in districts_detail:
        did = row["district_id"]
        dname = row.get("district_name") or "—"
        lines.append(f"\n• *District* `{did}` — {dname}")
        admins = row.get("open_administrations") or []
        if not admins:
            lines.append("    _No administration window open right now (checkbox synced to unchecked)._")
        else:
            for a in admins:
                lines.append(
                    f"    ◦ Administration `{a['administration_id']}` — {a['administration_name']}"
                )
    return "\n".join(lines)


def sync_open_assignments_from_airtable(*, dry_run: bool = False) -> dict:
    """
    For each row in the Airtable Dataset table, set Open Assignments from Firestore
    administrations (siteId on admin == district id; dateOpened/dateClosed).
    Slack lists all districts with open assignments when ``dry_run`` is True; when False,
    only districts whose Airtable row was updated.

    Only Airtable rows with **Redivis individual** checked are processed. If **Firestore siteId**
    is empty, the **Name** column is matched against ``districts.name``; on a unique match the
    site id is written to Airtable (or logged in dry run) before open-assignment sync.

    **Dry run (``dry_run=True``) — data stores are read-only**

    - **Airtable:** ``list_dataset_records()`` only. Never calls ``update_record_fields``
      or any other write.
    - **Firestore:** query ``administrations`` (``iter_administrations_for_site``) and read
      ``districts/{id}`` for display names only. No ``set`` / ``update`` / ``delete``.
    - **Slack:** always sends when ``dry_run`` is True (lists every processed site with an
      open assignment); otherwise only if at least one Airtable row was updated (list is
      only those sites). Incoming Webhook POST does not change Airtable,
      Firestore, GCS, or Redivis beyond what is described above.

    When ``dry_run`` is False, ``update_record_fields`` runs only when the cell
    must change: a missing Open Assignments field is treated as false, so no write
    if Firestore also has no open assignment.

    ``rows_updated`` counts only rows where Open Assignments **toggled** false ↔ true
    (field was present before; missing prior value is not a flip).
    """
    site_field = settings.config["AIRTABLE_FIELD_FIRESTORE_SITE_ID"]
    open_field = settings.config["AIRTABLE_FIELD_OPEN_ASSIGNMENTS"]
    redivis_field = settings.config["AIRTABLE_FIELD_REDIVIS_INDIVIDUAL"]
    site_name_field = settings.config["AIRTABLE_FIELD_SITE_NAME"]

    logging.info(
        "open_assignments_sync: starting dry_run=%s base=%s table=%s",
        dry_run,
        settings.config.get("AIRTABLE_LEVANTE_ENTITIES_BASE_ID"),
        settings.config.get("AIRTABLE_DATASET_TABLE_ID"),
    )

    airtable = AirtableServices()
    updated = []
    skipped_no_site = 0
    rows_unchanged = 0
    rows_written_or_would = 0
    rows_site_id_from_name = 0
    districts_with_open: list[dict] = []
    districts_airtable_changed: list[dict] = []

    # Redivis individual: prefer filterByFormula, then enforce in code.
    formula = f"{{{redivis_field}}}=1"
    try:
        raw_records = airtable.list_dataset_records(formula=formula)
    except Exception as e:
        logging.warning(
            "open_assignments_sync: filterByFormula failed (%s); fetching all rows and filtering",
            e,
        )
        raw_records = airtable.list_dataset_records()
    records = [
        r
        for r in raw_records
        if _airtable_checkbox_truthy((r.get("fields") or {}).get(redivis_field))
    ]
    logging.info(
        "open_assignments_sync: %s rows with %s (after filter, raw fetch %s)",
        len(records),
        redivis_field,
        len(raw_records),
    )

    for rec in records:
        rid = rec["id"]
        fields = rec.get("fields") or {}
        site_raw = fields.get(site_field)
        district_id: str | None = None
        resolved_site_id_from_name = False

        if site_raw and str(site_raw).strip():
            district_id = str(site_raw).strip()
        else:
            name_val = fields.get(site_name_field)
            name_str = str(name_val).strip() if name_val is not None else ""
            if not name_str:
                logging.info(
                    "open_assignments_sync: skip record=%s (no %r and no %r)",
                    rid,
                    site_field,
                    site_name_field,
                )
                skipped_no_site += 1
                continue
            looked = firestore_services.find_district_id_by_name(name_str)
            if not looked:
                logging.info(
                    "open_assignments_sync: skip record=%s (no district match for %s=%r)",
                    rid,
                    site_name_field,
                    name_str,
                )
                skipped_no_site += 1
                continue
            district_id = looked
            resolved_site_id_from_name = True

        open_admins = get_open_administrations_for_district(district_id)
        has_open = len(open_admins) > 0

        if has_open:
            dname = firestore_services.get_district_name(district_id)
            districts_with_open.append(
                {
                    "district_id": district_id,
                    "district_name": dname,
                    "open_administrations": open_admins,
                }
            )

        prev = fields.get(open_field)
        payload: dict = {}
        if resolved_site_id_from_name:
            payload[site_field] = district_id
        # Missing Open Assignments behaves like unchecked/false.
        if bool(prev) != has_open:
            payload[open_field] = has_open

        if not payload:
            rows_unchanged += 1
            continue

        if resolved_site_id_from_name:
            rows_site_id_from_name += 1

        if not dry_run:
            logging.info(
                "open_assignments_sync: updating Airtable record=%s district_id=%s payload_keys=%s",
                rid,
                district_id,
                sorted(payload.keys()),
            )
            if resolved_site_id_from_name:
                logging.info(
                    "open_assignments_sync: set %r from %s=%r -> district_id=%s",
                    site_field,
                    site_name_field,
                    fields.get(site_name_field),
                    district_id,
                )
            airtable.update_record_fields(rid, payload)
        else:
            logging.info(
                "open_assignments_sync: [dry_run] would update record=%s district_id=%s payload_keys=%s",
                rid,
                district_id,
                sorted(payload.keys()),
            )
            if resolved_site_id_from_name:
                logging.info(
                    "open_assignments_sync: [dry_run] would set %r from %s=%r -> district_id=%s",
                    site_field,
                    site_name_field,
                    fields.get(site_name_field),
                    district_id,
                )
        rows_written_or_would += 1
        districts_airtable_changed.append(
            {
                "district_id": district_id,
                "district_name": firestore_services.get_district_name(district_id),
                "open_administrations": open_admins,
            }
        )
        if _open_assignments_flipped(prev, has_open):
            updated.append(
                {
                    "record_id": rid,
                    "district_id": district_id,
                    "open_assignments": has_open,
                }
            )

    logging.info(
        "open_assignments_sync: loop done skipped_no_site_id=%s rows_unchanged=%s "
        "rows_written_or_would=%s rows_site_id_from_name=%s rows_open_assignments_flipped=%s "
        "unique_districts_with_open=%s",
        skipped_no_site,
        rows_unchanged,
        rows_written_or_would,
        rows_site_id_from_name,
        len(updated),
        len({d["district_id"] for d in districts_with_open}),
    )

    # De-dupe districts_with_open by district_id (same district may appear in multiple Airtable rows)
    seen = set()
    deduped: list[dict] = []
    for row in districts_with_open:
        d = row["district_id"]
        if d in seen:
            continue
        seen.add(d)
        deduped.append(row)
    districts_with_open = sorted(deduped, key=lambda x: x["district_id"])

    seen_changed = set()
    deduped_changed: list[dict] = []
    for row in districts_airtable_changed:
        d = row["district_id"]
        if d in seen_changed:
            continue
        seen_changed.add(d)
        deduped_changed.append(row)
    districts_changed_for_slack = sorted(deduped_changed, key=lambda x: x["district_id"])

    webhook_secret = _slack_webhook_secret_id_for_open_assignments(dry_run=dry_run)
    # Dry run: always report site open-assignment totals. Live run: only if Airtable changed.
    should_slack = dry_run or rows_written_or_would > 0
    if should_slack:
        slack_districts = (
            districts_with_open if dry_run else districts_changed_for_slack
        )
        msg = _format_open_assignments_slack_summary(
            dry_run=dry_run,
            rows_updated=len(updated),
            skipped_no_site_id=skipped_no_site,
            districts_detail=slack_districts,
        )
        logging.info(
            "open_assignments_sync: sending Slack summary (webhook_secret_id=%s)",
            webhook_secret,
        )
        notify_slack(msg, secret_id=webhook_secret)
    else:
        logging.info(
            "open_assignments_sync: skipping Slack (not dry_run and no Airtable row updates)"
        )
    logging.info("open_assignments_sync: finished dry_run=%s", dry_run)

    return {
        "dry_run": dry_run,
        "airtable_rows_total": len(records),
        "rows_updated": len(updated),
        "rows_unchanged": rows_unchanged,
        "skipped_no_site_id": skipped_no_site,
        "rows_site_id_resolved_from_name": rows_site_id_from_name,
        "updates": updated,
        "districts_with_open_assignments": districts_with_open,
    }
