import redivis
import os
import settings

os.environ["REDIVIS_API_TOKEN"] = settings.redivis_api_token


class RedivisServices:
    dataset_version = None

    def __init__(self, lab_id: str):
        self.organization = redivis.organization("LEVANTE")
        self.dataset = self.organization.dataset(name=lab_id)
        self.dataset_version = self.dataset.qualified_reference.split(":")[-1]

