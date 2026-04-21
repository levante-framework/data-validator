import logging

import settings
from shared.airtable_services import AirtableServices
from shared.slack_services import notify_slack
from validators.redivis_services import RedivisServices

logging.basicConfig(level=logging.INFO)


def _slack_secret_redivis(*, dry_run: bool) -> str:
    if dry_run:
        admin = (settings.config.get("SLACK_ADMIN_WEBHOOK_SECRET_ID") or "").strip()
        if admin:
            return admin
    return settings.config["SLACK_NOTIFICATION_WEB_HOOK"]


def _format_dry_run_slack(sites: list[dict]) -> str:
    lines = [
        "*[DRY RUN — read-only, full summary]* *Redivis individual ↔ datasets*",
        "Each row is an Airtable record with *Redivis individual* enabled.",
        "",
    ]
    if not sites:
        lines.append("_No matching Airtable rows._")
        return "\n".join(lines)

    for row in sites:
        rid = row["airtable_record_id"]
        label = row["site_label"]
        ds = row.get("redivis_dataset_name")
        if ds is None:
            lines.append(
                f"• *{label}* — Airtable `{rid}` — *empty `Name`* (must match Redivis dataset name)"
            )
            continue
        ex = row.get("dataset_exists")
        rel = row.get("is_released")
        tag = row.get("version_tag")
        deleted = row.get("is_deleted")
        ex_s = "yes" if ex else "no"
        rel_s = "yes" if rel else ("n/a" if not ex else "no")
        tag_s = f"`{tag}`" if tag else "—"
        del_s = f" deleted={deleted}" if deleted is not None else ""
        lines.append(
            f"• *{label}* — dataset `{ds}` — on Redivis: *{ex_s}* — released: *{rel_s}* — version {tag_s}{del_s} — Airtable `{rid}`"
        )
    return "\n".join(lines)


def _format_awaiting_slack(awaiting: list[dict]) -> str:
    lines = [
        "*Sites awaiting Redivis individual release* (flagged in Airtable, not released on Redivis):",
        "",
    ]
    for row in awaiting:
        lines.append(
            f"• {row['site_label']} — dataset `{row['redivis_dataset_name']}` (Airtable `{row['airtable_record_id']}`)"
        )
    return "\n".join(lines)


def check_redivis_individual_release_awaiting_slack(*, dry_run: bool = False) -> dict:
    """
    Sites where Airtable *Redivis individual* is true: compare to Redivis datasets.

    Normal mode: Slack only if at least one site is awaiting release (not present or not released).

    Dry run: always posts Slack with every matching site and Redivis status (exists / released / version).

    **Dry run (``dry_run=True``) — data stores are read-only**

    - **Airtable:** ``list_dataset_records()`` only. No create/update/delete.
    - **Redivis:** ``organization.dataset(name=...)`` then ``exists()`` / ``get()`` for
      properties (read-only API usage). No uploads, releases, or table mutations.
    - **Firestore:** not used on this path.
    - **Slack:** sends the dry-run summary (Webhook POST only).

    ``RedivisServices.__init__`` sets process ``os.environ`` for API tokens (local
    process only, not a remote datastore write).

    Redivis dataset lookup uses the Airtable **Name** field (``AIRTABLE_FIELD_REDIVIS_DATASET_NAME``),
    not **Firebase name**; it must match the dataset name on Redivis (there is no separate dataset id column).
    """
    individual_field = settings.config["AIRTABLE_FIELD_REDIVIS_INDIVIDUAL"]
    name_field = settings.config["AIRTABLE_FIELD_REDIVIS_DATASET_NAME"]

    logging.info(
        "redivis_individual_release: starting dry_run=%s base=%s table=%s fields(individual=%r name=%r)",
        dry_run,
        settings.config.get("AIRTABLE_LEVANTE_ENTITIES_BASE_ID"),
        settings.config.get("AIRTABLE_DATASET_TABLE_ID"),
        individual_field,
        name_field,
    )

    airtable = AirtableServices()
    awaiting: list[dict] = []
    sites_redivis_summary: list[dict] = []
    rs = RedivisServices()

    records = airtable.list_dataset_records()
    logging.info("redivis_individual_release: fetched %s total Airtable rows", len(records))

    rows_individual_flag = 0
    rows_empty_name = 0

    for rec in records:
        fields = rec.get("fields") or {}
        if not fields.get(individual_field):
            continue

        rows_individual_flag += 1
        name_raw = fields.get(name_field)
        dataset_name = str(name_raw).strip() if name_raw is not None else ""
        site_label = dataset_name or "(no name)"

        if not dataset_name:
            rows_empty_name += 1
            logging.warning(
                "redivis_individual_release: record %s has Redivis individual set but empty %r",
                rec["id"],
                name_field,
            )
            sites_redivis_summary.append(
                {
                    "airtable_record_id": rec["id"],
                    "site_label": site_label,
                    "redivis_dataset_name": None,
                    "dataset_exists": None,
                    "is_released": None,
                    "version_tag": None,
                    "is_deleted": None,
                }
            )
            continue

        rs.set_dataset(dataset_id=dataset_name)
        st = rs.get_current_dataset_status()
        logging.debug(
            "redivis_individual_release: record=%s dataset_name=%r exists=%s released=%s version=%s",
            rec["id"],
            dataset_name,
            st["exists"],
            st["is_released"],
            st.get("version_tag"),
        )

        row = {
            "airtable_record_id": rec["id"],
            "site_label": site_label,
            "redivis_dataset_name": dataset_name,
            "dataset_exists": st["exists"],
            "is_released": st["is_released"],
            "version_tag": st.get("version_tag"),
            "is_deleted": st.get("is_deleted"),
        }
        sites_redivis_summary.append(row)

        if st["exists"] and st["is_released"]:
            continue
        awaiting.append(
            {
                "airtable_record_id": rec["id"],
                "site_label": site_label,
                "redivis_dataset_name": dataset_name,
            }
        )

    logging.info(
        "redivis_individual_release: summary rows_with_individual_flag=%s empty_name=%s "
        "sites_checked=%s awaiting_release=%s",
        rows_individual_flag,
        rows_empty_name,
        len(sites_redivis_summary),
        len(awaiting),
    )

    slack_secret = _slack_secret_redivis(dry_run=dry_run)
    if dry_run:
        msg = _format_dry_run_slack(sites_redivis_summary)
        logging.info(
            "redivis_individual_release: sending dry_run Slack (webhook_secret_id=%s)",
            slack_secret,
        )
        notify_slack(msg, secret_id=slack_secret)
    elif awaiting:
        logging.info(
            "redivis_individual_release: sending awaiting-release Slack (webhook_secret_id=%s) count=%s",
            slack_secret,
            len(awaiting),
        )
        notify_slack(_format_awaiting_slack(awaiting), secret_id=slack_secret)
    else:
        logging.info(
            "redivis_individual_release: no Slack message (not dry_run and no sites awaiting release)"
        )

    logging.info("redivis_individual_release: finished dry_run=%s", dry_run)

    return {
        "dry_run": dry_run,
        "airtable_rows_total": len(records),
        "rows_with_redivis_individual": rows_individual_flag,
        "rows_empty_name": rows_empty_name,
        "awaiting_release_count": len(awaiting),
        "awaiting": awaiting,
        "sites_redivis_summary": sites_redivis_summary,
    }
