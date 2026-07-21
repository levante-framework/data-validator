import logging
import os

from pyairtable import Api

import settings
from shared.secret_services import secret_service

logging.basicConfig(level=logging.INFO)


class AirtableServices:
    """LEVANTE Entities base — Dataset table and related field access."""

    def __init__(self):
        token = secret_service.get_secret_payload(
            secret_id=settings.config["AIRTABLE_API_TOKEN_SECRET_ID"]
        )
        base_id = (
            settings.config.get("AIRTABLE_LEVANTE_ENTITIES_BASE_ID")
            or os.getenv("AIRTABLE_LEVANTE_ENTITIES_BASE_ID")
            or ""
        ).strip()
        table_id = settings.config["AIRTABLE_DATASET_TABLE_ID"]
        if not base_id:
            raise ValueError(
                "AIRTABLE_LEVANTE_ENTITIES_BASE_ID is not set in settings / environment."
            )
        self._api = Api(token)
        self._table = self._api.table(base_id, table_id)

    def list_dataset_records(self, *, formula: str | None = None) -> list[dict]:
        """
        List rows from the Dataset table. Optional ``formula`` is an Airtable
        ``filterByFormula`` string (e.g. checkbox field ``{Name}=1``).
        """
        if formula:
            return self._table.all(formula=formula)
        return self._table.all()

    def update_record_fields(self, record_id: str, fields: dict) -> dict:
        return self._table.update(record_id, fields)

    def find_dataset_record_by_name(self, name: str) -> dict | None:
        """Return the Dataset row whose ``Name`` equals ``name``, or None."""
        name = (name or "").strip()
        if not name:
            return None
        name_field = settings.config["AIRTABLE_FIELD_REDIVIS_DATASET_NAME"]
        escaped = name.replace("'", "''")
        formula = f"{{{name_field}}}='{escaped}'"
        rows = self.list_dataset_records(formula=formula)
        return rows[0] if rows else None

    def update_processed_dataset_last_update(
        self, *, processed_name: str, date_yyyy_mm_dd: str
    ) -> dict:
        """
        Write ``Redivis processed dataset last update`` for the row matching
        unmarked processed ``Name``.

        Returns ``{updated, record_id, error}``.
        """
        result = {"updated": False, "record_id": None, "error": None}
        field = settings.config["AIRTABLE_FIELD_PROCESSED_DATASET_LAST_UPDATE"]
        try:
            row = self.find_dataset_record_by_name(processed_name)
            if not row:
                result["error"] = (
                    f"No Airtable Dataset row with Name={processed_name!r}"
                )
                logging.error("update_processed_dataset_last_update: %s", result["error"])
                return result
            record_id = row["id"]
            self.update_record_fields(record_id, {field: date_yyyy_mm_dd})
            result["updated"] = True
            result["record_id"] = record_id
            logging.info(
                "update_processed_dataset_last_update: Name=%r %s=%s",
                processed_name,
                field,
                date_yyyy_mm_dd,
            )
        except Exception as e:
            result["error"] = str(e)
            logging.error(
                "update_processed_dataset_last_update(%r) failed: %s",
                processed_name,
                e,
            )
        return result
