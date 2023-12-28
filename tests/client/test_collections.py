from dnastack.client.collections.client import CollectionServiceClient, Collection, \
    UnknownCollectionError
from dnastack.client.data_connect import DataConnectClient
from dnastack.common.environments import flag
from tests.exam_helper import BasePublisherTestCase
from tests.exam_helper_for_data_connect import DataConnectTestCaseMixin


class TestCollectionsClient(BasePublisherTestCase, DataConnectTestCaseMixin):
    """ Test a client for Collection Service """

    @staticmethod
    def reuse_session() -> bool:
        return True

    def test_auth_client_interacts_with_collection_api(self):
        factory = self.get_factory()
        collection_client: CollectionServiceClient = CollectionServiceClient.make(
            [
                endpoint
                for endpoint in factory.all()
                if endpoint.type in CollectionServiceClient.get_supported_service_types()
            ][0]
        )

        assert collection_client is not None, f'The collection service client is not available. ({self.get_factory().all()})'

        collections = collection_client.list_collections()

        self.assertGreater(len(collections), 0)
        self.assertIsInstance(collections[0], Collection)
        collection = collections[0]
        self.assert_not_empty(collection.id)
        self.assert_not_empty(collection.slugName)

        with self.assertRaisesRegex(UnknownCollectionError, 'foo-bar'):
            collection_client.get('foo-bar')

    def test_auth_client_interacts_with_data_connect_api_without_collection(self):
        if not flag('TEST_WITH_PUBLISHER_DATA'):
            self.skipTest(
                'This test scenario requires the direct access to the collection service. The current test setup does not '
                'allow direct access to the collection service and therefore this test is not applicable.'
            )

        collection_client: CollectionServiceClient = self.get_factory().get('collection-service')
        data_connect_client = DataConnectClient.make(collection_client.data_connect_endpoint())
        for table in data_connect_client.list_tables():
            self.assert_not_empty([row for row in data_connect_client.query(f'SELECT * FROM "{table.name}" LIMIT 10')])

    def test_auth_client_interacts_with_data_connect_api_with_collection_for_backward_compatibility(self):
        collection_client: CollectionServiceClient = self.get_factory().get('collection-service')

        collections = collection_client.list_collections()
        self.assert_not_empty(collections)

        for target_collection in collections:
            data_connect_client = DataConnectClient.make(collection_client.data_connect_endpoint(target_collection))
            for table in data_connect_client.list_tables():
                table_name = '"' + ('"."'.join(table.name.split('.'))) + '"'
                test_query = f'SELECT * FROM {table_name} LIMIT 10'
                try:
                    self.assert_not_empty([row for row in data_connect_client.query(test_query)])
                    return  # Stop the test now.
                except AssertionError:
                    self._logger.warning(
                        f'T/{table.name}: Not usable for testing per-collection data connect as it is empty.'
                    )
                    continue

        self.fail('No collections are usable for this test scenario.')
