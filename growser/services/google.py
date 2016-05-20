import json

from apiclient.discovery import build
from apiclient.errors import HttpError
from httplib2 import Http
from oauth2client.client import SignedJwtAssertionCredentials


def _client(service_name, version, account_name, private_key, scope):
    auth = SignedJwtAssertionCredentials(account_name, private_key, scope)
    return build(service_name, version, http=auth.authorize(Http()))


class BaseService:
    def __init__(self, project_id, client_key):
        self.project_id = project_id

        with open(client_key) as fh:
            content = json.loads(fh.read())

        self.account_name = content['client_email']
        self.private_key = bytes(content['private_key'], 'UTF-8')
        self._client = None


class BigQueryService(BaseService):
    """Wrapper over google-api-client for working with BigQuery."""

    @property
    def client(self):
        if self._client is None:
            self._client = _client(
                    'bigquery', 'v2', self.account_name, self.private_key,
                    'https://www.googleapis.com/auth/bigquery')
        return self._client

    @property
    def jobs(self):
        return self.client.jobs()

    @property
    def tables(self):
        return self.client.tables()


class CloudStorageService(BaseService):
    """Wrapper over google-api-client for working with Cloud Storage."""

    @property
    def client(self):
        if self._client is None:
            self._client = _client(
                    'storage', 'v1', self.account_name, self.private_key,
                    'https://www.googleapis.com/auth/devstorage.full_control')
        return self._client

    @property
    def objects(self):
        return self.client.objects()

    @property
    def buckets(self):
        return self.client.buckets()


class BaseJob:
    def __init__(self, api):
        self.api = api
        self.project_id = self.api.project_id
        self.id = ''


HttpError = HttpError
