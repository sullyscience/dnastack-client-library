from typing import List
from urllib.parse import urljoin

from dnastack.context.manager import InvalidServiceRegistryError
from tests.cli.auth_utils import handle_device_code_flow
from tests.cli.base import PublisherCliTestCase


class TestCommand(PublisherCliTestCase):
    def setUp(self) -> None:
        super().setUp()
        self._temporarily_remove_existing_config()

    def test_crud(self):
        self.invoke('config', 'contexts', 'add', 'test-context')
        self.assertIn('test-context', self._get_context_names())

        self.invoke('config', 'contexts', 'add', 'test-context-002')
        self.assertIn('test-context-002', self._get_context_names())
        self.assertIn('test-context', self._get_context_names(), 'The previously added context should be there.')

        self.invoke('config', 'contexts', 'rename', 'test-context', 'test-context-001')
        self.assertNotIn('test-context', self._get_context_names(), 'The old context name should not be listed.')
        self.assertIn('test-context-001', self._get_context_names(), 'The new context name should be listed.')
        self.assertIn('test-context-002', self._get_context_names(), 'The previously added context should be there.')

        self.invoke('config', 'contexts', 'remove', 'test-context-002')
        self.assertIn('test-context-001', self._get_context_names(), 'The non-target context should be listed')
        self.assertNotIn('test-context-002', self._get_context_names(), 'The target context should not be listed')

    def test_use_command_with_auth_enabled_on_context_switch(self):
        self.prepare_for_device_code_flow(self._states.get('email'), self._states.get('token'))
        self.authenticate_with_device_code_flow([self._explorer_hostname, self._collection_service_hostname])

        secondary_context_host = self._collection_service_hostname

        self.invoke('use', '--name', 'test-viral-ai', self._explorer_hostname)
        self.assertGreaterEqual(len(self._get_context_names()), 1, 'There should be at least ONE context.')
        self.assertIn('test-viral-ai', self._get_context_names(), 'The target context should be listed')
        config = self._load_configuration()
        self.assertEqual(config.current_context, 'test-viral-ai')
        self.assertGreater(len(config.contexts[config.current_context].endpoints), 0,
                           'There should be at least one endpoints.')
        # ↓ This line should not throw an error or prompt for the authentication.
        self.simple_invoke('collections', 'list')

        handle_device_code_flow(['python', '-m', 'dnastack', 'use', secondary_context_host],
                                self._states['email'],
                                self._states['token'])
        self.assertGreaterEqual(len(self._get_context_names()), 2, 'There should be TWO contexts.')
        self.assertIn('test-viral-ai', self._get_context_names())
        self.assertIn(secondary_context_host, self._get_context_names())
        config = self._load_configuration()
        self.assertEqual(config.current_context, secondary_context_host)
        self.assertGreater(len(config.contexts[config.current_context].endpoints), 0,
                           'There should be at least one endpoints.')
        # ↓ This line should not throw an error or prompt for the authentication.
        self.invoke('collections', 'list', bypass_error=False)

        # Switch the context back should not be a problem.
        self.invoke('use', 'test-viral-ai', bypass_error=False)
        config = self._load_configuration()
        self.assertEqual(config.current_context, 'test-viral-ai')

    def test_use_command_with_auth_disabled_on_context_switch(self):
        # Switching to use the target context in this scenario should not prompt for the authentication.

        # The test will fail if the line below get timeout.
        self.invoke('use', self._explorer_hostname, '--no-auth', timeout=30, bypass_error=False)
        self.assertGreaterEqual(len(self._get_context_names()), 1, 'There should be at least ONE context.')
        self.assertIn(self._explorer_hostname, self._get_context_names(),
                      'The target context should be listed')
        config = self._load_configuration()
        self.assertEqual(config.current_context, self._explorer_hostname)
        self.assertGreater(len(config.contexts[config.current_context].endpoints), 0,
                           'There should be at least one endpoints.')
        # ↓ This line should not throw an error or prompt for the authentication.
        self.invoke('collections', 'list', '--no-auth')
        # self._show_config()

    def test_use_command_with_exact_url(self):
        self._temporarily_remove_existing_config()
        self.invoke('use', urljoin(self._explorer_base_url, '/api/service-registry'), '--no-auth', timeout=30, bypass_error=False)
        self.assertGreaterEqual(len(self._get_context_names()), 1, 'There should be ONE context.')
        self.assertIn(self._explorer_hostname, self._get_context_names(), 'The context should be registered.')
        config = self._load_configuration()
        self.assertEqual(config.current_context, self._explorer_hostname)
        self.assertGreater(len(config.contexts[config.current_context].endpoints), 0,
                           'There should be at least one endpoints.')
        # ↓ This line should not throw an error or prompt for the authentication.
        self.invoke('collections', 'list')
        # self._show_config()

    def test_use_command_error_recovery(self):
        # As described in #182492526
        for request_path in ('/foo/api', '/bar/api/service-registry'):
            self._temporarily_remove_existing_config()
            result = self.invoke('use', urljoin(self._explorer_base_url, request_path), '--no-auth', timeout=30,
                                 bypass_error=True)
            self.assertNotEqual(result.exit_code,
                                0,
                                f'Setting up a context with {request_path} should not return exit code 0, '
                                f'i.e., the command ran successfully.')

    def test_use_command_with_unusable_registry(self):

        with self.assert_exception_raised_in_chain(InvalidServiceRegistryError):
            self.invoke('use', 'http://localhost/')

        self.assertNotIn('localhost', self._get_context_names(), 'localhost shouldn\'t be a context since it is a invalid registry url')
        self.assertEqual(len(self._get_context_names()), 1)

    def show_output(self) -> bool:
        return False

    def _get_context_names(self) -> List[str]:
        return self.simple_invoke('config', 'contexts', 'list')
