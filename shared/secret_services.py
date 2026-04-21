import json
import os

from google.cloud import secretmanager
from functools import lru_cache


class SecretService:
    def __init__(self):
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = secretmanager.SecretManagerServiceClient()
        return self._client

    @lru_cache()
    def get_secret_payload(self, secret_id: str, version_id: str = "latest"):
        name = f"projects/{os.getenv('project_id')}/secrets/{secret_id}/versions/{version_id}"
        response = self.client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8")


secret_service = SecretService()
