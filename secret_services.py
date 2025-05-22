from google.cloud import secretmanager
import os
import settings


class _SecretServices:
    def __init__(self):
        self.client = secretmanager.SecretManagerServiceClient()
        self.project_id = os.getenv('project_id')
        self.setup_os_environ()

    def access_secret_version(self, secret_id, version_id):
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
        response = self.client.access_secret_version(request={"name": name})
        return response.payload.data.decode('UTF-8')

    def setup_os_environ(self):
        os.environ["REDIVIS_API_TOKEN"] = self.access_secret_version(
            secret_id=settings.config['REDIVIS_API_TOKEN_SECRET_ID'],
            version_id="latest")
        os.environ["ADMIN_API_KEY"] = self.access_secret_version(
            secret_id=settings.config['VALIDATOR_API_SECRET_ID'],
            version_id="latest")
        os.environ['ADMIN_SA'] = self.access_secret_version(
            secret_id=settings.config['ADMIN_SERVICE_ACCOUNT_SECRET_ID'], version_id="latest"
        )
        os.environ['ASSESSMENT_SA'] = self.access_secret_version(
            secret_id=settings.config['ASSESSMENT_SERVICE_ACCOUNT_SECRET_ID'], version_id="latest"
        )
        os.environ['REDIVIS_IDENTITY'] = self.access_secret_version(
            secret_id=settings.config['REDIVIS_IDENTITY_ACCOUNT_SECRET_ID'],
            version_id="latest")
        os.environ['CORE_DATA_BUCKET_NAME'] = f'levante-roar-data-bucket-{'dev' if 'dev' in os.environ['project_id'] else 'prod'}'


secret_services = _SecretServices()
