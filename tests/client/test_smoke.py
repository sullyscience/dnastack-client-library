from typing import Any, Dict, Iterator, Optional, List
from urllib.parse import urljoin

from dnastack import CollectionServiceClient, DataConnectClient
from dnastack.client.collections.client import EXPLORER_COLLECTION_SERVICE_TYPE_V1_0
from dnastack.client.data_connect import TableNotFoundError, TableInfo
from dnastack.client.models import ServiceEndpoint
from dnastack.client.result_iterator import ResultIterator
from dnastack.common.logger import get_logger

from tests.exam_helper import BasePublisherTestCase


class TestSmokeWithoutAuthentication(BasePublisherTestCase):
    _logger = get_logger('lib/smoke_test')

    @staticmethod
    def automatically_authenticate() -> bool:
        return False

    def test_ga4gh_demo(self):
        """ Test the demo scenario documented in https://github.com/ga4gh-discovery/data-connect/blob/gh-pages/hugo/content/docs/getting-started/consume-data.md """
        # init search client
        bucket_url = 'https://storage.googleapis.com/ga4gh-tables-example/'
        client = DataConnectClient.make(ServiceEndpoint(url=bucket_url))

        # get tables
        tables = client.list_tables()

        # get table proxy object
        table_name = tables[0].name
        table = client.table(table_name)

        # get table info
        self.assertIsInstance(table.info, TableInfo)

        # get table data
        result_iterator = table.data
        self.assertIsInstance(result_iterator, ResultIterator)
        data = [r for r in result_iterator]
        self.assertIsInstance(data, list)
        self.assert_not_empty(data)
        self.assertIsInstance(data[0], dict)


class TestSmokeWithAuthentication(BasePublisherTestCase):
    _logger = get_logger('lib/smoke_test')

    def test_demo(self):
        """
        This is based on the public documentation.

        .. note:: This test is specifically designed for a certain deployment.
        """
        endpoint = ServiceEndpoint(url=urljoin(self._explorer_base_url, '/api/'),
                                   type=EXPLORER_COLLECTION_SERVICE_TYPE_V1_0)
        client = CollectionServiceClient.make(endpoint)

        self._logger.debug('Listing collections...')
        collections = self._get_testable_collections(client)
        self.assertGreater(len(collections), 0, f'{endpoint.url} should have at least ONE collection.')

        for collection_index in range(len(collections)):
            target_collection = collections[collection_index]
            self._logger.debug(f'Use the {target_collection.slugName} collection')

            try:
                data_connect = DataConnectClient.make(client.data_connect_endpoint(target_collection))

                self._logger.debug('Listing tables...')
                tables = data_connect.list_tables()
                self.assertGreater(len(tables), 0, f'{target_collection.name} should have at least ONE table.')

                usable_tables = []

                self._logger.debug(f'Checking if this collection is good for testing with table(s) {[t.name for t in tables]}')

                for listed_table in tables:
                    target_table_name = listed_table.name
                    self._logger.debug(f'Use T/{target_table_name}')

                    table = data_connect.table(listed_table)

                    table_info = table.info
                    self.assertEqual(target_table_name, table_info.name)
                    self.assert_not_empty(table_info.data_model['properties'])

                    self._logger.debug(f'Testing with T/{table_info.name}')

                    try:
                        self.assert_not_empty(self._get_subset_of(table.data, 100), 'Failed to fetch the data from table.')
                    except TableNotFoundError as e:
                        self._logger.warning(f'T/{target_table_name}: Encountered unexpected error while interacting with /table/{target_table_name}/data')
                        self._logger.warning(f'T/{target_table_name}: {type(e).__module__}.{type(e).__name__}: {e}')
                        if usable_tables > 0:
                            self._logger.warning(f'There exists {len(usable_tables)} tables good enough for testing.')
                            break
                        else:
                            raise RuntimeError(f'No usable tables for testing on /table/{target_table_name}/data')

                    usable_tables.append(target_table_name)

                # We will try just for a small subset of tables.
                queried_tables = [r for r in data_connect.query(
                    # language=sql
                    f"SELECT item.* FROM ({target_collection.itemsQuery}) item WHERE type='table' LIMIT 5"
                )]
                queried_table_count = len(queried_tables)

                table_index = 0

                while table_index < len(queried_tables):
                    target_table_name = queried_tables[table_index]['qualified_table_name']

                    if len(target_table_name.split(r'.')) < 3:
                        target_table_name = f'ncbi_sra.{target_table_name}'

                    self._logger.debug(f'Querying from {target_table_name}...')
                    query = f'SELECT * FROM {target_table_name} LIMIT 20000'

                    try:
                        rows = self._get_subset_of(data_connect.query(query))
                    except Exception as e:
                        self._logger.warning(f'T/{target_table_name}: Encountered unexpected error while interacting with /search on {target_table_name}')
                        self._logger.warning(f'T/{target_table_name}: {type(e).__module__}.{type(e).__name__}: {e}')
                        if table_index < queried_table_count - 1:
                            self._logger.warning('Try the next table...')
                            table_index += 1
                            continue
                        else:
                            raise RuntimeError(f'No usable tables for testing on /search')

                    if len(rows) == 0:
                        self._logger.warning(f'T/{target_table_name}: No data.')
                        if table_index < queried_table_count - 1:
                            self._logger.warning('Try the next table...')
                            table_index += 1
                            continue
                        else:
                            raise RuntimeError(f'No usable tables for testing on /search')
                    else:
                        break  # Found a usable table. Break the loop.

                if table_index == len(queried_tables):
                    raise RuntimeError(f"Cannot test as there is no data in any of {', '.join([t['qualified_table_name'] for t in queried_tables])}")

                return  # End the test here.
            except Exception as e:
                self._logger.warning(f'C/{target_collection.slugName}: This collection is not usable for testing.')
                self._logger.warning(f'C/{target_collection.slugName}: Reason: {type(e).__module__}.{type(e).__name__}: {e}')

        # At this point, this means there are no usable tests. Throw an error.
        self.fail('No usable collections for this test')

    def _get_subset_of(self, iterator: Iterator[Dict[str, Any]], max_size: Optional[int] = None) -> List[Dict[str, Any]]:
        rows = []

        for row in iterator:
            rows.append(row)

            if max_size and len(rows) >= max_size:
                break

            if len(rows) % 10000 == 0:
                self._logger.debug(f'Receiving {len(rows)} rows...')

            self.assertGreater(len(row.keys()), 0)

        self._logger.debug(f'Received {len(rows)} row(s)')

        return rows
