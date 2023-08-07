from typing import Optional

from dagster_gcp import GCSResource as DagsterGCSResource
from google.cloud import storage
from google.oauth2 import service_account
from pydantic import Field


class GCSResource(DagsterGCSResource):
    file: Optional[str] = Field(default=None, description="Credentials file")

    def get_client(self) -> storage.Client:
        credentials = service_account.Credentials.from_service_account_file(self.file)
        return storage.client.Client(project=self.project, credentials=credentials)
