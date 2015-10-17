import random
import unittest
from unittest.mock import MagicMock, Mock
import uuid

from apiclient.errors import HttpError

from growser.services.bigquery import _table
from growser.services.bigquery import DeleteTable
from growser.services.bigquery import ExecuteAsyncQuery
from growser.services.bigquery import ExecuteQuery
from growser.services.bigquery import ExportTableToCSV
from growser.services.bigquery import FetchQueryResults
from growser.services.bigquery import PersistQueryToTable
from growser.services.bigquery import QueryResult


PROJECT_ID = "test_project_id"


class BigQueryServiceTestCase(unittest.TestCase):
    def service(self):
        return MagicMock(project_id=PROJECT_ID)

    def test_DeleteTable(self):
        service = self.service()
        table = "table.to_delete"

        job = DeleteTable(service)
        success = job.run(table)

        service.tables.delete.assert_called_with(**_table(PROJECT_ID, table))
        self.assertTrue(job.is_complete)
        self.assertTrue(success)

    def test_DeleteTable_not_exist(self):
        service = self.service()
        table = "table.to_delete"

        service.tables.delete = Mock(side_effect=HttpError('url', b'content'))

        job = DeleteTable(service)
        success = job.run(table)

        self.assertFalse(success)

    def test_ExecuteAsyncQuery(self):
        service = self.service()
        query = "SELECT * FROM ds.table LIMIT 100"

        ExecuteAsyncQuery(service).run(query)

        self.assertIn(query, str(service.mock_calls[0]))

    def test_ExecuteQuery(self):
        service = self.service()
        query = "SELECT * FROM ds.table LIMIT 100"

        ExecuteQuery(service).run(query)

        expected = {"body": {"query": query}, "projectId": PROJECT_ID}
        service.jobs.query.assert_called_once_with(**expected)

    def test_ExportTableToCSV(self):
        service = self.service()
        source = "source.table"
        destination = "gs://some-path/files.gz"

        ExportTableToCSV(service).run(source, destination)

        extract = service.mock_calls[0][2]['body']['configuration']['extract']
        check1 = extract['sourceTable']
        check2 = extract['destinationUris']

        self.assertEqual(service.mock_calls[0][0], "jobs.insert")
        self.assertEqual(check1, _table(PROJECT_ID, source))
        self.assertIn(destination, check2)

    def test_PersistQueryToTable(self):
        service = self.service()
        query = "SELECT * FROM ds.table LIMIT 100"
        table = "ds.export_table"

        PersistQueryToTable(service).run(query, table)

        body = service.jobs.insert.call_args_list[0][1]['body']['configuration']['query']
        expected = _table(PROJECT_ID, table)

        service.tables.delete.assert_called_once_with(**expected)
        self.assertEqual(expected, body['destinationTable'])
        self.assertEqual(query, body['query'])

    def test_QueryResult(self):
        output = response_example(False)
        result = QueryResult(iter([output]))

        rows = list(result.rows())
        rows_dict = list(result.rows(True))

        expected_fields = ['repo_id', 'actor_id', 'event']

        self.assertEqual(result.total_rows, len(output['rows']))
        self.assertEqual(len(rows), len(output['rows']))
        self.assertEqual(len(rows), len(result))

        self.assertEqual(result.fields, expected_fields)
        for field in expected_fields:
            self.assertIn(field, rows_dict[0])


def response_example(token=False, errors=False):
    def random_rows(num):
        result = []
        for i in range(num):
            result.append({'f': [
                {'v': random.randint(1,1000)},
                {'v': random.randint(1000,2000)},
                {'v': random.choice(['WatchEvent', 'ForkEvent'])}
            ]})
        return result

    rows = random_rows(random.randint(1, 10))
    response = {
        'cacheHit': False,
        'jobComplete': True,
        'jobReference': {
            'jobId': 'job_K7wQRG0iaQbT4Y',
            'projectId': PROJECT_ID
        },
        'kind': 'bigquery#queryResponse',
        'rows': rows,
        'schema': {
            'fields': [
                {'mode': 'NULLABLE', 'name': 'repo_id', 'type': 'INTEGER'},
                {'mode': 'NULLABLE', 'name': 'actor_id', 'type': 'INTEGER'},
                {'mode': 'NULLABLE', 'name': 'event', 'type': 'STRING'}
            ]
        },
        'status': {'state': 'DONE'},
        'totalBytesProcessed': '1234567',
        'totalRows': len(rows)
    }

    if errors:
        response['status'] = {'errors': ["test"]}

    if token:
        response['pageToken'] = uuid.uuid4()

    return response
