from apiclient.discovery import build
from apiclient.errors import HttpError
from httplib2 import Http
from oauth2client.client import SignedJwtAssertionCredentials


def _client(service_name, version, account_name, private_key, scope):
    auth = SignedJwtAssertionCredentials(account_name, private_key, scope)
    return build(service_name, version, http=auth.authorize(Http()))


class BigQueryService(object):
    """Wrapper over google-api-client for working with BigQuery."""
    def __init__(self, project_id: str, account_name: str, private_key: bytes):
        self.project_id = project_id
        self.client = _client(
                'bigquery', 'v2', account_name, private_key,
                'https://www.googleapis.com/auth/bigquery')

    @property
    def jobs(self):
        return self.client.jobs()

    @property
    def tables(self):
        return self.client.tables()


class CloudStorageService(object):
    """Wrapper over google-api-client for working with Cloud Storage."""
    def __init__(self, project_id: str, account_name: str, private_key: bytes):
        self.project_id = project_id
        self.client = _client(
                'storage', 'v1', account_name, private_key,
                'https://www.googleapis.com/auth/devstorage.full_control')

    @property
    def objects(self):
        return self.client.objects()

    @property
    def buckets(self):
        return self.client.buckets()


class BaseJob(object):
    def __init__(self, api):
        self.api = api
        self.project_id = self.api.project_id
        self.id = ''


HttpError = HttpError
