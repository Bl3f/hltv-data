import base64
import json

from typing import Optional
from pydantic import Field

from dagster_gcp import GCSResource as DagsterGCSResource
from google.cloud import storage
from google.oauth2 import service_account


class GCSResource(DagsterGCSResource):
    service_account_json: Optional[str] = Field(default=None, description="Credentials file content encoded in base64")

    def get_client(self) -> storage.Client:
        decoded_service_account_json = json.loads(base64.b64decode(self.service_account_json))
        credentials = service_account.Credentials.from_service_account_info(decoded_service_account_json)
        return storage.client.Client(project=self.project, credentials=credentials)
