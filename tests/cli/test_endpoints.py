from urllib.parse import urljoin

from tests.cli.base import PublisherCliTestCase


class TestCommand(PublisherCliTestCase):
    @staticmethod
    def automatically_authenticate() -> bool:
        return False

    def test_happy_path(self):
        # Add a public endpoint.
        self.invoke('config', 'endpoints', 'add', 'sample-viral-ai', '-t', 'collections')
        self._configure_endpoint(
            'sample-viral-ai',
            {
                'url': urljoin(self._explorer_base_url, '/api/'),
            }
        )

        # The first endpoint of each type would become the default endpoint of that type automatically.
        self._assert_if_the_default_endpoint_is('collections', 'sample-viral-ai')

        # Adding a new endpoint with the same ID should raise an error.
        self.expect_error_from(['config', 'endpoints', 'add', 'sample-viral-ai', '-t', 'collections'],
                               r'^EndpointAlreadyExists: sample-viral-ai')

        # Add the second endpoint of the same type.
        self.invoke('config', 'endpoints', 'add', 'sample-viral-ai-secondary', '-t', 'collections')

        # The default endpoint of this type remains the same.
        self._assert_if_the_default_endpoint_is('collections', 'sample-viral-ai')

        # The default endpoint is switched to the secondary.
        self.invoke('config', 'endpoints', 'set-default', 'sample-viral-ai-secondary')
        self._assert_if_the_default_endpoint_is('collections', 'sample-viral-ai-secondary')

        # The default endpoint is switched to the secondary.
        self.invoke('config', 'endpoints', 'unset-default', 'sample-viral-ai-secondary')
        self.assertIsNone(self.simple_invoke('config', 'endpoints', 'get-defaults').get('collections'))

        # Setting an unknown property of a registered endpoint should raise an error.
        self.expect_error_from(['config', 'endpoints', 'set', 'sample-viral-ai', 'foo.bar', 'panda'],
                               error_message='InvalidConfigurationProperty: foo.bar')

        # Setting an unknown property of an unregistered endpoint should raise an error.
        self.expect_error_from(['config', 'endpoints', 'set', 'snake', 'foo.bar', 'panda'],
                               error_message='EndpointNotFound: snake')

        # Add a data connect endpoint with partial authentication information
        self.invoke('config', 'endpoints', 'add', 'sample-data-connect', '-t', 'data_connect')
        self._configure_endpoint(
            'sample-data-connect',
            {
                'url': urljoin(self._collection_service_url, '/data-connect/'),
                'authentication.type': 'oauth2',
                'authentication.client_id': 'faux-client-id',
                'authentication.client_secret': 'faux-client-secret',
                'authentication.grant_type': 'client_credentials',
            }
        )

        # Setting an unknown "authentication" property of a registered endpoint should raise an error.
        self.expect_error_from(['config', 'endpoints', 'set', 'sample-data-connect', 'authentication.foo_bar', 'panda'],
                               error_message='InvalidConfigurationProperty: authentication.foo_bar')

        # Remove the data connect endpoint.
        self.invoke('config', 'endpoints', 'remove', 'sample-data-connect')
        with self.assertRaises(IndexError):
            # This is to confirm that the endpoint has been removed.
            self._get_endpoint('sample-data-connect')

        # Removing twice should not raise an error.
        self.invoke('config', 'endpoints', 'remove', 'sample-data-connect')

    def _assert_if_the_default_endpoint_is(self, short_type: str, expected_endpoint_id: str):
        self.assertEqual(self.simple_invoke('config', 'endpoints', 'get-defaults')[short_type], expected_endpoint_id)
