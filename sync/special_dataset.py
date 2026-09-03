"""Build and run a multi-site validator dataset from Airtable Special Dataset rows."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import settings
from shared import utils
from shared.airtable_services import AirtableServices
from shared.slack_services import notify_slack
from validators.data_validation_pipeline import run_data_validation
from validators.redivis_services import RedivisServices


def _field(name: str) -> str:
    return settings.config[name]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _single(value: Any, *, field_name: str) -> Any:
    """Unwrap Airtable linked-record arrays that must contain one value."""
    if isinstance(value, list):
        if len(value) != 1:
            raise ValueError(
                f"{field_name} must contain exactly one value; got {value!r}"
            )
        return value[0]
    return value


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _text(value).casefold() in {"1", "true", "yes", "checked"}


def _date(value: Any, *, field_name: str) -> str:
    text = _text(value)
    if not text:
        raise ValueError(f"{field_name} is required")
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    raise ValueError(
        f"{field_name} must be YYYY-MM-DD or M/D/YYYY; got {text!r}"
    )


def _user_filter(value: Any) -> dict | None:
    if value in (None, ""):
        return None
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        raise ValueError(f"user_filter must be JSON text or an object; got {value!r}")
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid user_filter JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("user_filter JSON must decode to an object")
    return parsed


def _user_limit(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"user_limit must be a positive integer; got {value!r}") from exc
    if numeric < 1 or not numeric.is_integer():
        raise ValueError(f"user_limit must be a positive integer; got {value!r}")
    return int(numeric)


def _row_to_org(row: dict, airtable: AirtableServices) -> utils.Organization:
    fields = row.get("fields") or {}
    linked_record_id = _text(
        _single(
            fields.get(_field("AIRTABLE_SPECIAL_FIELD_DATASET_LINK")),
            field_name="dataset_link",
        )
    )
    dataset_link = ""
    if linked_record_id:
        linked = airtable.get_dataset_record(linked_record_id)
        dataset_link = _text(
            (linked.get("fields") or {}).get(
                settings.config["AIRTABLE_FIELD_REDIVIS_DATASET_NAME"]
            )
        )
    firestore_org_id = _text(
        _single(
            fields.get(_field("AIRTABLE_SPECIAL_FIELD_ORG_ID")),
            field_name="org_id",
        )
    )
    is_guest = _bool(fields.get(_field("AIRTABLE_SPECIAL_FIELD_IS_GUEST")))
    parsed_user_filter = _user_filter(
        fields.get(_field("AIRTABLE_SPECIAL_FIELD_USER_FILTER"))
    )
    if not dataset_link:
        if is_guest and parsed_user_filter:
            dataset_link = f"guest-{_text(parsed_user_filter.get('value'))}"
        else:
            raise ValueError(f"row {row.get('id')} has no dataset_link")
    if not firestore_org_id and not is_guest:
        raise ValueError(f"row {row.get('id')} has no org_id")

    filters: dict[str, Any] = {
        "date_filter": {
            "start_date": _date(
                fields.get(_field("AIRTABLE_SPECIAL_FIELD_START_DATE")),
                field_name="start_date",
            ),
            "end_date": _date(
                fields.get(_field("AIRTABLE_SPECIAL_FIELD_END_DATE")),
                field_name="end_date",
            ),
        },
    }
    if firestore_org_id:
        filters["org_filter"] = {
            "key": "districts",
            "operator": "array_contains_any",
            "value": [firestore_org_id],
        }
    if parsed_user_filter:
        filters["user_filter"] = parsed_user_filter

    return utils.Organization(
        org_id=dataset_link,
        is_guest=is_guest,
        user_number_limit=_user_limit(
            fields.get(_field("AIRTABLE_SPECIAL_FIELD_USER_LIMIT"))
        ),
        filters=filters,
    )


def _common_value(rows: list[dict], setting_key: str) -> str:
    field = _field(setting_key)
    values = {
        _text((row.get("fields") or {}).get(field))
        for row in rows
        if _text((row.get("fields") or {}).get(field))
    }
    if len(values) > 1:
        raise ValueError(
            f"matching Airtable rows disagree on {field}: {sorted(values)}"
        )
    return next(iter(values), "")


def _format_slack(result: dict) -> str:
    verification = result.get("processed_verification") or {}
    process = (result.get("validation") or {}).get("process_dataset") or {}
    status = "succeeded" if result.get("success") else "failed"
    lines = [
        (
            f"*Special dataset validation"
            f"{' [TEST MODE]' if result.get('test_mode') else ''}* — {status}"
        ),
        "",
        f"• Airtable configuration: `{result.get('dataset_name') or '—'}`",
        f"• Raw output dataset: `{result.get('output_dataset_name') or '—'}`",
        f"• Processed dataset: `{result.get('processed_name') or '—'}`",
        f"• Airtable rows / validator orgs: {result.get('row_count', 0)}",
        f"• Raw Redivis release: {'yes' if (result.get('validation') or {}).get('new_version_release') else 'no'}",
        f"• `process_dataset`: {'completed' if process.get('ran') and not process.get('error') else 'failed/skipped'}",
    ]
    release = process.get("processed_release") or {}
    if release.get("released"):
        lines.append(
            f"• Processed Redivis release: `{release.get('before_version') or '—'}` → "
            f"`{release.get('after_version') or '—'}`"
        )
    elif release.get("skip_reason") == "release_processed_dataset=false":
        lines.append(
            "• Processed Redivis release: skipped (release_processed_dataset=false)"
        )
    elif release.get("skipped"):
        lines.append(
            f"• Processed Redivis release: already at "
            f"`{release.get('after_version') or '—'}`"
        )
    elif release.get("error"):
        lines.append(f"• Processed Redivis release: failed — {release.get('error')}")
    if result.get("test_mode"):
        lines.append("• Airtable processed_ref_id verification/writeback: skipped")
    else:
        lines.extend(
            [
                (
                    "• Processed ref: "
                    f"`{verification.get('actual_ref_id') or '—'}` "
                    f"(Airtable match: {verification.get('ref_matches')})"
                ),
                (
                    "• Processed version: "
                    f"`{verification.get('before_version') or 'none'}` → "
                    f"`{verification.get('after_version') or 'none'}` "
                    f"(updated: {verification.get('version_updated')})"
                ),
                (
                    "• Airtable processed_ref_id backfilled: "
                    f"{verification.get('rows_backfilled', 0)}"
                ),
            ]
        )
    errors = result.get("errors") or []
    if process.get("error"):
        errors = [*errors, f"process_dataset: {process['error']}"]
    if errors:
        lines.extend(["", "*Errors*"])
        lines.extend(f"• {error}" for error in errors[:10])
    return "\n".join(lines)


def run_special_dataset_validation(
    dataset_name: str,
    *,
    test_mode: bool = False,
    test_dataset_name: str | None = None,
) -> dict:
    """
    Read Special Dataset rows, run one forced multi-org raw release, process it,
    then verify the configured processed dataset reference and version changed.

    In test mode, Airtable still supplies the real query scopes, but output is
    redirected to an explicit ``TEST-...-raw`` dataset. Airtable reference
    verification and writes are skipped.
    """
    dataset_name = _text(dataset_name)
    test_dataset_name = _text(test_dataset_name)
    output_dataset_name = test_dataset_name if test_mode else dataset_name
    result: dict[str, Any] = {
        "operation": "special_dataset_validation",
        "dataset_name": dataset_name,
        "output_dataset_name": output_dataset_name,
        "test_mode": test_mode,
        "processed_name": None,
        "row_count": 0,
        "validation": None,
        "processed_verification": None,
        "success": False,
        "errors": [],
    }

    try:
        if not dataset_name:
            raise ValueError("dataset_name is required")
        suffix = settings.config["RAW_DATASET_SUFFIX"]
        if not dataset_name.endswith(suffix):
            raise ValueError(
                f"dataset_name must be a raw dataset ending with {suffix!r}"
            )
        if test_mode:
            if not test_dataset_name:
                raise ValueError("test_dataset_name is required when test_mode=true")
            if not test_dataset_name.startswith("TEST-"):
                raise ValueError("test_dataset_name must start with 'TEST-'")
            if not test_dataset_name.endswith(suffix):
                raise ValueError(
                    f"test_dataset_name must end with {suffix!r}"
                )

        airtable = AirtableServices()
        rows = airtable.list_special_dataset_records(dataset_name)
        result["row_count"] = len(rows)
        if not rows:
            raise ValueError(
                f"No Special Dataset Airtable rows found for {dataset_name!r}"
            )

        configured_processed_name = _common_value(
            rows, "AIRTABLE_SPECIAL_FIELD_PROCESSED_NAME"
        ) or RedivisServices.processed_name_from_raw(dataset_name)
        expected_processed = RedivisServices.processed_name_from_raw(dataset_name)
        if configured_processed_name != expected_processed:
            raise ValueError(
                f"processed_name must be {expected_processed!r} for raw "
                f"{dataset_name!r}; got {configured_processed_name!r}"
            )
        processed_name = RedivisServices.processed_name_from_raw(
            output_dataset_name
        )
        result["processed_name"] = processed_name

        orgs = [_row_to_org(row, airtable) for row in rows]

        configured_ref = ""
        before: dict = {}
        before_ref = None
        rs = RedivisServices()
        if not test_mode:
            configured_ref = _common_value(
                rows, "AIRTABLE_SPECIAL_FIELD_PROCESSED_REF_ID"
            )
            rs.set_dataset(dataset_id=processed_name)
            before = rs.get_current_dataset_status()
            before_ref = rs.get_reference_id()

        params = utils.DatasetParameters(
            dataset_id=output_dataset_name,
            is_save_to_storage=True,
            is_force_uploading_to_redivis=True,
            send_slack=False,  # This operation posts one combined summary.
            orgs=orgs,
        )
        body, status = run_data_validation(
            params,
            slack_org_progress=False,
            slack_summary_always=False,
        )
        validation = json.loads(body)
        result["validation"] = validation
        if status != 200:
            raise RuntimeError(f"data_validation returned HTTP {status}")

        process = validation.get("process_dataset") or {}
        if test_mode:
            if not validation.get("new_version_release"):
                result["errors"].append(
                    "test raw dataset did not produce a new released version"
                )
            if not process.get("ran") or process.get("error"):
                result["errors"].append(
                    "process_dataset did not complete successfully"
                )
            result["success"] = not result["errors"]
            return result

        rs.set_dataset(dataset_id=processed_name)
        after = rs.get_current_dataset_status()
        actual_ref = rs.get_reference_id()
        ref_matches = bool(
            actual_ref
            and (not configured_ref or configured_ref == actual_ref)
            and (not before_ref or before_ref == actual_ref)
        )
        version_updated = bool(
            process.get("ran")
            and not process.get("error")
            and after.get("is_released")
            and after.get("version_tag") != before.get("version_tag")
        )

        rows_backfilled = 0
        ref_field = _field("AIRTABLE_SPECIAL_FIELD_PROCESSED_REF_ID")
        if actual_ref and ref_matches:
            for row in rows:
                current = _text((row.get("fields") or {}).get(ref_field))
                if not current:
                    airtable.update_special_dataset_record_fields(
                        row["id"], {ref_field: actual_ref}
                    )
                    rows_backfilled += 1

        result["processed_verification"] = {
            "configured_ref_id": configured_ref or None,
            "actual_ref_id": actual_ref,
            "ref_matches": ref_matches,
            "before_version": before.get("version_tag"),
            "after_version": after.get("version_tag"),
            "is_released": after.get("is_released"),
            "version_updated": version_updated,
            "rows_backfilled": rows_backfilled,
        }
        if not ref_matches:
            result["errors"].append(
                "processed_ref_id does not match the processed Redivis dataset"
            )
        if not version_updated:
            result["errors"].append(
                "processed dataset did not produce a new released version"
            )
        result["success"] = not result["errors"]
    except Exception as exc:
        logging.exception("special_dataset_validation failed")
        result["errors"].append(f"{type(exc).__name__}: {exc}")
    finally:
        try:
            notify_slack(_format_slack(result))
        except Exception:
            logging.exception("special_dataset_validation Slack notification failed")

    return result
