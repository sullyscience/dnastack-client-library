from typing import List

from dnastack import DataConnectClient
from dnastack.client.collections.client import CollectionServiceClient
from dnastack.common.events import Event
from tests.exam_helper import BasePublisherTestCase, EventCollector


class TestOAuth2AuthenticatorIntegrationTest(BasePublisherTestCase):
    @staticmethod
    def reuse_session() -> bool:
        return True

    def test_auth_info_consolidation(self):
        """
        Test auth info consolidation.

        In this scenario, there should be only one authentication initiated.
        """
        event_collector = EventCollector(['authentication-before',
                                          'authentication-ok',
                                          'authentication-failure',
                                          'refresh-before',
                                          'refresh-ok',
                                          'refresh-failure',
                                          'session-restored',
                                          'session-not-restored',
                                          'session-revoked'])
        factory = self.get_factory()
        cs: CollectionServiceClient = CollectionServiceClient.make(
            [
                endpoint
                for endpoint in factory.all()
                if endpoint.type in CollectionServiceClient.get_supported_service_types()
            ][0]
        )
        dc: DataConnectClient = DataConnectClient.make(
            [
                endpoint
                for endpoint in factory.all()
                if endpoint.type in DataConnectClient.get_supported_service_types()
            ][0]
        )

        event_collector.prepare_for_interception(cs)
        event_collector.prepare_for_interception(dc)

        # Trigger the authentication and confirm that some clients are still working normally.
        self.assert_not_empty([collection.slugName for collection in cs.list_collections()])  # First round
        self.assert_not_empty([row for row in dc.query('SELECT 1 AS x')])  # Second round
        self.assert_not_empty([row for row in dc.query('SELECT 3 AS x')])  # Third round

        event_sequence = [s.type for s in event_collector.sequence]

        try:
            self.assertEqual(event_sequence,
                             ['session-not-restored', 'authentication-before', 'authentication-ok'],
                             'Unexpected auth sequence for client-credential flow')
        except AssertionError:
            self.assertIn('session-restored',
                          event_sequence,
                          'Unexpected auth sequence when the client has already had valid sessions')

    def __create_event_interceptor(self, client_events: List[str], event_type):
        def intercept(event: Event):
            self._logger.debug(f'Intercepted E/{event_type}: {event}')
            client_events.append(event_type)

        return intercept
