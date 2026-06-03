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
