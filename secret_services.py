from google.cloud import secretmanager
import os


class _SecretServices:
    project_id = os.environ.get('project_id', None)

    def __init__(self):
        self.client = secretmanager.SecretManagerServiceClient()

    def access_secret_version(self, secret_id, version_id):
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
        response = self.client.access_secret_version(request={"name": name})
        return response.payload.data.decode('UTF-8')


secret_services = _SecretServices()
