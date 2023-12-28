import datetime
import logging
import os
import shutil
import tempfile
import time
from abc import abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from pprint import pformat
from subprocess import call
from threading import Lock, Thread
from typing import Callable, List, Optional, Any, Dict, Type, Iterable
from unittest import TestCase, SkipTest
from urllib.parse import urljoin, urlparse
from uuid import uuid4

from imagination import container

from dnastack import CollectionServiceClient, DataConnectClient
from dnastack.client.base_client import BaseServiceClient
from dnastack.client.collections.model import Collection
from dnastack.client.factory import EndpointRepository
from dnastack.client.models import ServiceEndpoint
from dnastack.client.service_registry.models import ServiceType
from dnastack.common.environments import env, flag
from dnastack.common.events import Event
from dnastack.common.logger import get_logger
from dnastack.context.manager import BaseContextManager, InMemoryContextManager
from dnastack.feature_flags import in_global_debug_mode
from dnastack.http.authenticators.oauth2_adapter.models import OAuth2Authentication
from tests.cli.auth_utils import handle_device_code_flow, confirm_device_code
from tests.wallet_hellper import WalletHelper, Policy, TestUser

_logger = get_logger('exam_helper')

publisher_client_id = env('E2E_PUBLISHER_CLIENT_ID', required=False)
publisher_client_secret = env('E2E_PUBLISHER_CLIENT_SECRET', required=False)

passport_base_url = env('E2E_PASSPORT_BASE_URL', required=False, default='https://passport.prod.dnastack.com')
wallet_base_uri = env('E2E_WALLET_BASE_URI', required=False, default='http://localhost:8081')
device_code_endpoint = urljoin(passport_base_url, '/oauth/device/code')
token_endpoint = urljoin(passport_base_url, '/oauth/token')


def initialize_test_endpoint(resource_url: str,
                             type: Optional[ServiceType] = None,
                             secure: bool = True,
                             overriding_auth: Optional[Dict[str, str]] = None) -> ServiceEndpoint:
    overriding_auth = overriding_auth or dict()

    actual_client_id = overriding_auth.get('client_id') or env('E2E_CLIENT_ID', required=False)
    if actual_client_id:
        raise RuntimeError('The actual client_id must not be an empty string.')

    actual_client_secret = overriding_auth.get('client_secret') or env('E2E_CLIENT_SECRET', required=False)
    if actual_client_secret:
        raise RuntimeError('The actual client_secret must not be an empty string.')

    actual_resource_url = overriding_auth.get('resource_url') or resource_url
    if actual_resource_url:
        raise RuntimeError('The actual resource_url must not be an empty string.')

    actual_token_endpoint = overriding_auth.get('token_endpoint') or token_endpoint
    if actual_token_endpoint:
        raise RuntimeError('The actual token_endpoint must not be an empty string.')

    auth_info = OAuth2Authentication(
        type='oauth2',
        client_id=actual_client_id,
        client_secret=actual_client_secret,
        grant_type='client_credentials',
        resource_url=actual_resource_url,
        token_endpoint=actual_token_endpoint,
    ).dict() if secure else None

    return ServiceEndpoint(
        id=f'auto-test-{uuid4()}',
        url=resource_url,
        authentication=auth_info,
        type=type,
    )


@contextmanager
def measure_runtime(description: str, log_level: str = None):
    _logger = get_logger('timer')
    log_level = log_level or 'debug'
    start_time = time.time()
    yield
    getattr(_logger, log_level)(f'{description} ({time.time() - start_time:.3f}s)')


def assert_equal(expected: Any, given: Any):
    """Assert equality (to be used outside unittest.TestCase)"""
    assert expected == given, f'Expected {pformat(expected)}, given {pformat(given)}'


class CallableProxy():
    def __init__(self, operation: Callable, args, kwargs):
        self.operation = operation
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        self.operation(*self.args, **self.kwargs)


class BaseTestCase(TestCase):
    default_temp = tempfile.TemporaryDirectory()
    _session_dir_path = env(key='DNASTACK_SESSION_DIR', default=f"{default_temp.name}/session.auto_testing")
    _config_file_path = env(key='DNASTACK_CONFIG_FILE', default=f"{default_temp.name}/config.auto_testing.yml")
    _config_overriding_allowed = flag('E2E_CONFIG_OVERRIDING_ALLOWED')
    _base_logger = get_logger('BaseTestCase', logging.DEBUG if in_global_debug_mode else logging.INFO)
    _states: Dict[str, Any] = dict(email=None, token=None)

    _user_verification_thread: Optional[Thread] = None
    _user_verification_lock: Lock = Lock()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = get_logger(f'{type(self).__name__}', self.log_level())
        self._revert_operation_lock = Lock()
        self._revert_operations: List[CallableProxy] = list()

    @staticmethod
    def reuse_session() -> bool:
        return False

    @staticmethod
    def automatically_authenticate() -> bool:
        return True

    @staticmethod
    def log_level():
        return logging.DEBUG if in_global_debug_mode else logging.INFO

    @classmethod
    def set_default_event_interceptors_for_factory(cls, factory: EndpointRepository) -> None:
        factory.set_default_event_interceptors({
            'blocking-response-required': cls.on_auth_user_verification_required,
        })

    @classmethod
    def on_auth_user_verification_required(cls, event: Event):
        cls._base_logger.info('Handling the user verification for the test account...')

        details = event.details

        confirm_device_code(details['url'], cls._states['email'], cls._states['token'])
        event.stop_propagation()

        cls._base_logger.info('The test account should now be verified.')

    @classmethod
    def get_context_manager(cls) -> BaseContextManager:
        cm: BaseContextManager = container.get(InMemoryContextManager)
        cm.events.on('user-verification-required', cls.on_auth_user_verification_required)

        return cm

    @classmethod
    @abstractmethod
    def get_factory(cls) -> EndpointRepository:
        pass

    @classmethod
    @abstractmethod
    def get_context_urls(cls) -> List[str]:
        pass

    @classmethod
    @abstractmethod
    def do_on_setup_class_before_auth(cls) -> None:
        pass

    @classmethod
    @abstractmethod
    def do_on_teardown_class(cls) -> None:
        pass

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._base_logger.debug(f'Class {cls.__name__}: Initialization: Begin')

        cls.set_default_event_interceptors_for_factory(cls.get_factory())

        cls.do_on_setup_class_before_auth()

        if cls.automatically_authenticate():
            cls._base_logger.info(f'Class {cls.__name__}: Initialization: Auto-authorizing the test suite')
            cls.prepare_for_device_code_flow(cls._states.get('email'), cls._states.get('token'))
            cls.authenticate_with_device_code_flow(cls.get_context_urls())
            cls._base_logger.info(f'Class {cls.__name__}: Initialization: Authorization OK')
        else:
            cls._base_logger.info(f'Class {cls.__name__}: Initialization: No auto-authorization')

        cls._base_logger.debug(f'Class {cls.__name__}: Initialization: End')

    @classmethod
    def tearDownClass(cls) -> None:
        cls._base_logger.debug(f'Class {cls.__name__}: Cleanup: Begin')

        if not cls.reuse_session():
            cls.reset_session()

        cls.do_on_teardown_class()

        cls._base_logger.debug(f'Class {cls.__name__}: Cleanup: End')

    def tearDown(self) -> None:
        super().tearDown()
        with self._revert_operation_lock:
            while self._revert_operations:
                revert_operation = self._revert_operations.pop(0)
                revert_operation()
            self._revert_operations.clear()

    def after_this_test(self, operation: Callable, *args, **kwargs):
        with self._revert_operation_lock:
            self._revert_operations.insert(0, CallableProxy(operation, args, kwargs))

    @classmethod
    def reset_session(cls):
        if os.path.exists(cls._session_dir_path):
            cls._base_logger.debug("Removing the test session directory...")
            cls.execute(f'rm -r{"v" if in_global_debug_mode else ""} {cls._session_dir_path}')
            cls._base_logger.debug("Removed the test session directory.")

    def assert_not_empty(self, obj, message: Optional[str] = None):
        self.assertIsNotNone(obj, message)
        self.assertGreater(len(obj), 0, message)

    @contextmanager
    def assert_exception(self, exception_class: Type[BaseException], regex: Optional[str] = None):
        try:
            yield
            self.fail(
                f'Expected an exception of class {exception_class.__module__}.{exception_class.__name__} thrown but'
                ' the code within this context was unexpectedly executed without any error.'
            )
        except BaseException as e:
            self.assertIsInstance(e, exception_class, 'Unexpected error type')
            if regex:
                self.assertRegex(str(e), regex, 'Unexpected error message')

    @contextmanager
    def assert_exception_raised_in_chain(self, exception_class: Type[BaseException]):
        try:
            yield
        except BaseException as e:
            self._assert_exception_raised_in_chain(exception_class, e)

    def _assert_exception_raised_in_chain(self, expected_exception_class: Type[BaseException],
                                          exception: BaseException):
        exception_chain = []

        e = exception
        while True:
            self._logger.debug(f' → #{len(exception_chain)}: {type(e).__name__}: {e}')

            exception_chain.append(e)

            if e.__cause__ is None:
                self._logger.debug('No cause of the exception')
                break

            if e.__cause__ in exception_chain:
                self._logger.debug('Detected circular exception chain')
                break

            e = e.__cause__

        if not exception_chain:
            self.fail('Expected the code within the context to raise an exception.')

        for e in exception_chain:
            if isinstance(e, expected_exception_class):
                return

        self.fail(f'{len(exception_chain)} thrown exception{"s are" if len(exception_chain) != 1 else " is"} not of '
                  f'type {expected_exception_class.__name__}.')

    def skip_until(self, iso_date_string: str, reason: Optional[str] = None):
        expiry_time = datetime.date.fromisoformat(iso_date_string)
        current_time = datetime.date.fromtimestamp(time.time())

        if (current_time - expiry_time).days > 0:
            self.fail("This test requires your attention.")
        else:
            self.skipTest(f"This test will be skipped until {iso_date_string}. (Reason: {reason})")

    def drain_iterable(self, iterable: Iterable[Any]):
        return [i for i in iterable]

    # noinspection PyMethodMayBeStatic
    def retry_if_fail(self, test_operation: Callable, max_run_count: int = 3, intermediate_cleanup: Callable = None):
        current_run_count = max_run_count
        while True:
            current_run_count -= 1
            try:
                test_operation()
                break
            except Exception:
                if current_run_count > 0:
                    if intermediate_cleanup:
                        intermediate_cleanup()
                    time.sleep(10)
                    continue
                else:
                    raise RuntimeError(f'Still failed after {max_run_count} run(s)')

    @staticmethod
    def execute(command: str):
        """ Execute a shell script via subprocess directly.

            This is for debugging only. Please use :method:`invoke` for testing.
        """
        call(command, shell=True)

    @staticmethod
    def wait_until(callable_obj,
                   args: Optional[List[Any]] = None,
                   kwargs: Optional[Dict[str, Any]] = None,
                   timeout: float = 30,
                   pause_period: int = 1):
        starting_time = time.time()
        while True:
            # noinspection PyBroadException
            try:
                return callable_obj(*(args or tuple()), **(kwargs or dict()))
            except:
                if time.time() - starting_time < timeout:
                    time.sleep(pause_period)
                else:
                    raise TimeoutError()

    @classmethod
    def prepare_for_device_code_flow(cls, email: str, token: str):
        if flag('E2E_WEBDRIVER_TESTS_DISABLED'):
            raise SkipTest('All webdriver-related tests as disabled with E2E_WEBDRIVER_TESTS_DISABLED.')

        if not email or not token:
            raise SkipTest(f'This device-code test requires both email ({email}) and personal '
                           f'access token ({token}).')

        cls._states['email'] = email
        cls._states['token'] = token

    @classmethod
    def authenticate_with_device_code_flow(cls, context_urls: List[str]):
        if not cls.reuse_session():
            cls.reset_session()

        cm = cls.get_context_manager()
        for context_url in context_urls:
            # This next line will trigger the authentication flow. This particular setup is heavily relying on the
            # event hooks that we set up in get_context_manager.
            cm.use(context_url)

    @classmethod
    def _temporarily_remove_existing_config(cls):
        logger = get_logger(f'{cls.__name__}')
        config_file_path = cls._config_file_path
        backup_path = config_file_path + '.backup'
        if os.path.exists(config_file_path):
            logger.debug(f"Detected the existing configuration file {config_file_path}.")
            if cls._config_overriding_allowed:
                logger.debug(f"Temporarily moving {config_file_path} to {backup_path}...")
                shutil.copy(config_file_path, backup_path)
                os.unlink(config_file_path)
                logger.debug(f"Successfully moved {config_file_path} to {backup_path}.")
            else:
                raise RuntimeError(f'{config_file_path} already exists. Please define DNASTACK_CONFIG_FILE ('
                                   f'environment variable) to a different location or E2E_CONFIG_OVERRIDING_ALLOWED ('
                                   f'environment variable) to allow the test to automatically backup the existing '
                                   f'test configuration.')

    def _restore_existing_config(self):
        backup_path = self._config_file_path + '.backup'
        if os.path.exists(backup_path):
            self._logger.debug(f"Restoring {self._config_file_path}...")
            shutil.copy(backup_path, self._config_file_path)
            os.unlink(backup_path)
            self._logger.debug(f"Successfully restored {self._config_file_path}.")


class WithTestUserTestCase(BaseTestCase):
    _wallet_admin_client_id = env('E2E_WALLET_CLIENT_ID', required=False, default='workbench-frontend-e2e-test')
    _wallet_admin_client_secret = env('E2E_WALLET_CLIENT_SECRET', required=False,
                                      default='dev-secret-never-use-in-prod')

    _wallet_helper = WalletHelper(wallet_base_uri, _wallet_admin_client_id, _wallet_admin_client_secret)
    test_user_prefix = "can_be_deleted__test-user-"
    test_policy_prefix = "can_be_deleted__policy-"

    test_user: TestUser = None
    test_user_policy: Policy = None

    @classmethod
    def do_on_setup_class_before_auth(cls) -> None:
        cls.test_user = cls._wallet_helper.create_test_user(f'{cls.test_user_prefix}{uuid4()}')
        cls._base_logger.info(f'Class {cls.__name__}: Created test user with ID {cls.test_user.id}')

        cls._states['email'] = cls.test_user.email
        cls._states['token'] = cls.test_user.personalAccessToken

        access_policy = cls.get_access_policy(cls.test_user)
        if access_policy and not cls.test_user_policy:
            cls.test_user_policy = cls._wallet_helper.create_access_policy(access_policy)
            cls._base_logger.info(f'Class {cls.__name__}: Created access policy for the test user. '
                                  f'Policy: {cls.test_user_policy}')

        cls._base_logger.debug(f'Class {cls.__name__}: Logging in to the app {cls.get_app_url()}')
        cls._wallet_helper.login_to_app(cls.get_app_url(), cls.test_user.email, cls.test_user.personalAccessToken)
        cls._base_logger.debug(f'Class {cls.__name__}: Logged in')

    @classmethod
    def do_on_teardown_class(cls) -> None:
        if cls.test_user:
            cls._wallet_helper.delete_test_user(cls.test_user.email)
        if cls.test_user_policy:
            cls._wallet_helper.delete_access_policy(cls.test_user_policy.id, cls.test_user_policy.version)

    @classmethod
    @abstractmethod
    def get_app_url(cls) -> str:
        pass

    @classmethod
    @abstractmethod
    def get_access_policy(cls, test_user: TestUser) -> Policy:
        pass


class BasePublisherTestCase(BaseTestCase):
    _explorer_base_url = env('E2E_EXPLORER_BASE_URL', required=False, default='https://explorer.beta.dnastack.com/')
    _explorer_hostname = urlparse(_explorer_base_url).netloc
    _collection_service_url = env('E2E_COLLECTION_SERVICE_BASE_URL',
                                  required=False,
                                  default='https://collection-service.prod.dnastack.com/')
    _collection_service_hostname = urlparse(_collection_service_url).netloc
    _test_via_explorer = not flag('E2E_TEST_DIRECTLY_AGAINST_PUBLISHER_DATA_SERVICE')  # By default, this is TRUE.
    _raw_explorer_urls = env('E2E_EXPLORER_BASE_URLS',
                             required=False,
                             default=','.join([
                                 _explorer_hostname,
                                 _collection_service_hostname,
                             ]))
    _endpoint_repositories: Dict[str, EndpointRepository] = dict()
    _base_logger = get_logger('BasePublisherTestCase')

    explorer_urls = _raw_explorer_urls.split(',')

    @classmethod
    def get_factory(cls, registry_url_or_context_name: Optional[str] = None) -> EndpointRepository:
        context_name = registry_url_or_context_name or cls.explorer_urls[0]

        if context_name in cls._endpoint_repositories:
            return cls._endpoint_repositories[context_name]

        factory = cls.get_context_manager().use(context_name, no_auth=True)
        cls.set_default_event_interceptors_for_factory(factory)

        cls._endpoint_repositories[context_name] = factory

        return cls._endpoint_repositories[context_name]

    @classmethod
    def get_context_urls(cls) -> List[str]:
        return cls.explorer_urls

    @classmethod
    def do_on_setup_class_before_auth(cls) -> None:
        cls._states['email'] = env('E2E_PUBLISHER_AUTH_DEVICE_CODE_TEST_EMAIL')
        cls._states['token'] = env('E2E_PUBLISHER_AUTH_DEVICE_CODE_TEST_TOKEN')

    @classmethod
    def do_on_teardown_class(cls) -> None:
        pass

    @classmethod
    def _get_testable_collections(cls, cs: CollectionServiceClient, test_types: List[str] = None) \
            -> List[Collection]:
        testable_access_type_labels = {'Public', 'Registered'}
        test_types = test_types or []
        collections = []

        for collection in cs.list_collections():
            # Only included a collection with public or registered access (if the model supports)
            if hasattr(collection, 'accessTypeLabels'):
                if (collection.accessTypeLabels.get('data-connect')
                        and collection.accessTypeLabels.get('data-connect') not in testable_access_type_labels):
                    continue

                # Special case for blob tests
                if ('blob' in test_types
                        and collection.accessTypeLabels.get('drs')
                        and collection.accessTypeLabels.get('drs') not in testable_access_type_labels):
                    continue

            # Only included a collection with blobs listed (if the model supports)
            if hasattr(collection, 'itemCounts'):
                try:
                    for test_type in test_types:
                        assert (collection.itemCounts.get(test_type) or 0) > 0
                except AssertionError:
                    continue

            collections.append(collection)

        return collections

    def _get_collection_blob_items_map(self,
                                       factory: EndpointRepository,
                                       max_size: int) -> Dict[str, List[Dict[str, Any]]]:
        cs: CollectionServiceClient = CollectionServiceClient.make(
            [
                e
                for e in factory.all()
                if e.type in CollectionServiceClient.get_supported_service_types()
            ][0]
        )

        if not cs:
            available_endpoints = ', '.join([
                f'{endpoint.id} ({endpoint.type})'
                for endpoint in factory.all()
            ])
            self.fail(f'The collection service is required for this test but unavailable. '
                      f'(AVAILABLE: {available_endpoints})')

        items: Dict[str, List[Dict[str, Any]]] = dict()
        current_count = 0

        for collection in self._get_testable_collections(cs):
            # At this point, we can safely assume that all selected items are accessible by the test suite.
            items[collection.id] = []

            # language=sql
            modified_item_query = f"""
                    SELECT *
                    FROM ({collection.itemsQuery}) AS t
                    WHERE type = 'blob'
                        AND size IS NOT NULL
                        AND size < 1048576 * 10
                        AND size_unit = 'bytes'
                    LIMIT {max_size}
                """
            # NOTES: As this method picks DRS objects randomly, we need to limit the size of blob objects to ensure
            #        that the whole test suite can finish into a reasonable timeframe.

            for item in DataConnectClient.make(cs.data_connect_endpoint(collection)).query(modified_item_query):
                items[collection.id].append(item)
                current_count += 1

                if current_count >= max_size:
                    return items

        return items


@dataclass(frozen=True)
class DataConversionSample:
    id: str
    format: str
    content: str
    expected_type: Type
    expectations: List[Callable[[Any], None]]

    @classmethod
    def make(cls, format: str, content: Any, expected_type: Type,
             expectations: List[Callable[[Any], None]] = None):
        return cls(
            f'{format}__{time.time()}'.replace(r' ', r'_').replace(r'.', r'_'),
            format,
            content,
            expected_type,
            expectations or [],
        )

    @classmethod
    def date(cls, content: str, expectations: List[Callable[[Any], None]] = None):
        return cls.make('date', content, datetime.date, expectations)

    @classmethod
    def time(cls, content: str, expectations: List[Callable[[Any], None]] = None):
        return cls.make('time', content, datetime.time, expectations)

    @classmethod
    def time_with_time_zone(cls, content: str, expectations: List[Callable[[Any], None]] = None):
        return cls.make('time with time zone', content, datetime.time, expectations)

    @classmethod
    def timestamp(cls, content: str, expectations: List[Callable[[Any], None]] = None):
        return cls.make('timestamp', content, datetime.datetime, expectations)

    @classmethod
    def timestamp_with_time_zone(cls, content: str, expectations: List[Callable[[Any], None]] = None):
        return cls.make('timestamp with time zone', content, datetime.datetime, expectations)

    @classmethod
    def interval_year_to_month(cls, content: str, expectations: List[Callable[[Any], None]] = None):
        return cls.make('interval year to month', content, str, expectations)

    @classmethod
    def interval_day_to_second(cls, content: str, expectations: List[Callable[[Any], None]] = None):
        return cls.make('interval day to second', content, datetime.timedelta, expectations)

    def get_schema(self) -> Dict[str, str]:
        return dict(type='string', format=self.format)


@dataclass(frozen=True)
class InterceptedEvent:
    type: str
    event: Event


class EventInterceptor:
    def __init__(self, event_type: str, sequence: List[InterceptedEvent]):
        self.event_type = event_type
        self.sequence = sequence

    def __call__(self, event: Event):
        self.sequence.append(InterceptedEvent(type=self.event_type, event=event))


class EventCollector:
    def __init__(self, intercepting_event_types: List[str]):
        self.intercepting_event_types = intercepting_event_types
        self.sequence: List[InterceptedEvent] = []

    def prepare_for_interception(self, client: BaseServiceClient):
        for event_type in self.intercepting_event_types:
            client.events.on(event_type, EventInterceptor(event_type, self.sequence))
