from collections import Sized
from io import FileIO
from itertools import chain
import os

from apiclient.discovery import build
from apiclient.http import MediaIoBaseDownload
from apiclient.errors import HttpError
from httplib2 import Http
from oauth2client.client import SignedJwtAssertionCredentials


def _client(service_name, version, account_name, private_key, scope):
    auth = SignedJwtAssertionCredentials(account_name, private_key, scope)
    return build(service_name, version, http=auth.authorize(Http()))


class BaseService:
    """Authentication required for all Google API services."""
    service_name = None
    version = None
    scope = None

    def __init__(self, project_id, account_name, private_key):
        self.project_id = project_id
        self.account_name = account_name
        self.private_key = private_key
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = _client(self.service_name, self.version,
                                   self.account_name, self.private_key,
                                   self.scope)
        return self._client


class BigQueryService(BaseService):
    """Wrapper over google-api-client for working with BigQuery."""
    service_name = 'bigquery'
    version = 'v2'
    scope = 'https://www.googleapis.com/auth/bigquery'

    @property
    def jobs(self):
        return self.client.jobs()

    @property
    def tables(self):
        return self.client.tables()


class CloudStorageService(BaseService):
    """Wrapper over google-api-client for working with Cloud Storage."""
    service_name = 'storage'
    version = 'v1'
    scope = 'https://www.googleapis.com/auth/devstorage.full_control'

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


class BigQueryJob(BaseJob):
    num_retries = 5

    @property
    def info(self):
        if not self.id:
            raise JobNotCompleteException('Job not complete')
        return self._call('get', jobId=self.id)

    @property
    def is_complete(self):
        return self.info['status']['state'] == 'DONE'

    def query(self, body: dict):
        return self._call('query', body=body)

    def insert(self, body: dict):
        return self._call('insert', body=body)

    def _call(self, api: str, **kwargs):
        """Execute a request against the Google API."""
        func = getattr(self.api.jobs, api)(projectId=self.project_id, **kwargs)
        rv = func.execute(num_retries=self.num_retries)
        if 'jobReference' in rv:
            self.id = rv['jobReference']['jobId']
        if 'errors' in rv['status']:
            raise JobFailedException(rv['status']['errors'])
        return rv


class ExecuteQuery(BigQueryJob):
    def run(self, query: str):
        """Execute a query and immediately return the results.

        :param query: Query to execute on BigQuery.
        """
        results = self.query({'query': query})
        return QueryResult(iter([results]))


class ExecuteAsyncQuery(BigQueryJob):
    def run(self, query: str):
        """Execute a query in batch mode to be retrieved later.

        :param query: Query to run in batch mode.
        """
        body = {
            'configuration': {
                'query': {
                    'query': query,
                    'priority': 'BATCH'
                }
            }
        }
        return self.insert(body)

    def results(self):
        return FetchQueryResults(self.api).run(self.id)


class FetchQueryResults(BigQueryJob):
    def run(self, job_id: str):
        """Fetch all results from a query stored on BigQuery."""
        self.id = job_id
        if not self.is_complete:
            raise JobNotCompleteException('Job is not complete')
        return QueryResult(self._pages())

    def _pages(self):
        """Return all pages of results for the query."""
        kwargs = {'jobId': self.id}
        has_token = True
        while has_token:
            rv = self._call('getQueryResults', **kwargs)
            has_token = 'pageToken' in rv
            if has_token:
                kwargs['pageToken'] = rv['pageToken']
            yield rv


class DeleteTable(BigQueryJob):
    def run(self, table: str):
        """Delete a table hosted on BigQuery.

        :param table: Name of the table to delete.
        """
        try:
            self.api.tables.delete(**_table(self.project_id, table)).execute()
        except HttpError:
            return False
        return True

    @property
    def is_complete(self):
        """API has empty response, assume true."""
        return True


class PersistQueryToTable(BigQueryJob):
    """Execute a query and save the results to a table."""
    def run(self, query: str, destination: str):
        DeleteTable(self.api).run(destination)
        body = {
            'configuration': {
                'query': {
                    'query': query,
                    'allowLargeResults': True,
                    'destinationTable': _table(self.project_id, destination)
                }
            }
        }
        return self.insert(body)


class ExportTableToCSV(BigQueryJob):
    """Export a table to Google Cloud Storage as compressed CSV files."""
    def run(self, source: str, destination: str):
        body = {
            'configuration': {
                'extract': {
                    'sourceTable': _table(self.project_id, source),
                    'destinationUris': [destination],
                    'destinationFormat': 'CSV',
                    'compression': 'GZIP'
                }
            }
        }
        return self.insert(body)


class QueryResult(Sized):
    def __init__(self, pages):
        self._pages = pages
        self._first = next(self._pages)
        self.fields = [f['name'] for f in self._first['schema']['fields']]
        self.total_rows = int(self._first['totalRows'])

    def rows(self, as_dict: bool=False):
        def to_tuple(row):
            return list(map(lambda x: x['v'], row['f']))

        def to_dict(row):
            return dict(zip(self.fields, to_tuple(row)))

        for response in chain([self._first], self._pages):
            transform = to_dict if as_dict else to_tuple
            yield from (transform(row) for row in response['rows'])

    def __len__(self):
        return self.total_rows


class JobNotCompleteException(Exception):
    pass


class JobFailedException(Exception):
    pass


def _table(project_id, table):
    id1, id2 = table.split('.')
    return {'projectId': project_id, 'datasetId': id1, 'tableId': id2}


class GoogleStorageJob(BaseJob):
    """Base for any job that runs against Google Cloud Storage."""


class DownloadFile(GoogleStorageJob):
    """Download a file from a Google Cloud Storage bucket to a local path."""
    def run(self, bucket: str, obj: str, local_path: str):
        archive = self.api.objects.get_media(bucket=bucket, object=obj)
        filename = os.path.join(local_path, os.path.basename(obj))
        with FileIO(filename, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, archive, chunksize=1024*1024)
            complete = False
            while not complete:
                _, complete = downloader.next_chunk()
        return filename


class DeleteFile(GoogleStorageJob):
    """Delete a file from a Google Cloud Storage bucket"""
    def run(self, bucket: str, obj: str):
        try:
            self.api.objects.delete(bucket=bucket, object=obj).execute()
            return True
        except HttpError:
            # Error is returned if the object does not exist - can ignore
            return False


class FindFilesMatchingPrefix(GoogleStorageJob):
    """Return a list of all files matching `prefix`."""
    def run(self, bucket: str, prefix: str):
        response = self.api.objects \
            .list(bucket=bucket, prefix=prefix).execute()
        return [i for i in response['items'] if int(i['size']) > 0]


class DownloadBucketPath(GoogleStorageJob):
    """Download a Google Storage bucket to a local path."""
    def run(self, bucket: str, bucket_path: str, local_path: str):
        archives = FindFilesMatchingPrefix(self.api).run(bucket, bucket_path)
        filenames = []
        for file in archives:
            filenames.append(DownloadFile(self.api).run(
                bucket, file['name'], local_path))
            DeleteFile(self.api).run(bucket, file['name'])
        return filenames
