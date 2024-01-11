from google.cloud import secretmanager
import os
import settings


class SecretServices:
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', settings.project_id)
    if 'local' in settings.DB_SITE:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = settings.SA_KEY_LOCATION_ADMIN

    def __init__(self):
        self.client = secretmanager.SecretManagerServiceClient()

    def access_secret_version(self, secret_id, version_id):
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
        response = self.client.access_secret_version(request={"name": name})
        return response.payload.data.decode('UTF-8')
