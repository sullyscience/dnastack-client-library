import json
import os
from typing import TypeVar, Iterator, List

from dnastack.alpha.client.wes.client import WesClient, STANDARD_WES_TYPE_V1_0, RunRequest, Run
from dnastack.common.environments import env, EnvironmentVariableRequired, flag
from tests.exam_helper import BasePublisherTestCase, initialize_test_endpoint

T = TypeVar('T')


class TestClient(BasePublisherTestCase):
    wes: WesClient

    @staticmethod
    def reuse_session() -> bool:
        return True

    def setUp(self):
        super(TestClient, self).setUp()

        if not flag('E2E_WES_TEST_ENABLED'):
            self.skipTest('Test disabled... Set E2E_WES_TEST_ENABLED to true to enable.')

        try:
            self._endpoint_uri = env('E2E_WES_ENDPOINT_URI',
                                     required=False,
                                     default=f'https://workspaces-wes.alpha.dnastack.com/ga4gh/wes/v1/')
            self._client_id = env('E2E_STAGING_CLIENT_ID',
                                  required=False,
                                  default='dnastack-client-library-testing')
            self._client_secret = env('E2E_STAGING_CLIENT_SECRET',
                                      description='OAuth Client Secret for E2E staging',
                                      required=False)
            self._token_endpoint = env('E2E_STAGING_TOKEN_URI',
                                       required=False,
                                       default='https://wallet.staging.dnastack.com/oauth/token')
            self._resource_url = env('E2E_WES_OAUTH_RESOURCE_URI',
                                     required=False,
                                     default='https://workspaces-wes.alpha.dnastack.com/')
        except EnvironmentVariableRequired as e:
            self.skipTest(f'The required environment variable ({e.args[0]}) is not set.')

        self.wes = WesClient.make(
            initialize_test_endpoint(
                self._endpoint_uri,
                type=STANDARD_WES_TYPE_V1_0,
                overriding_auth=dict(
                    client_id=self._client_id,
                    client_secret=self._client_secret,
                    token_endpoint=self._token_endpoint,
                    resource_url=self._resource_url,
                )
            )
        )

    def test_read_only_operations(self):
        wes = self.wes

        info = wes.get_service_info()
        self.assertIn('1.0', info['workflow_type_versions']['WDL'])

        # Listing runs without changing page size should throw no error.
        runs = wes.get_runs()
        self._limit_to(runs, 50)

        # Listing runs with specific page size should also throw no error.
        runs = wes.get_runs(page_size=5)
        self._limit_to(runs, 20)

    def _get_sample_file_path(self, *path):
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        return os.path.join(base_path, 'samples', 'workflows', *path)

    def test_file_input(self):
        self._run_test(RunRequest(
            workflow_url='file_input.wdl',
            workflow_params=self._load_json('file_input', 'inputs.json'),
            tags={'origin_lang': 'python', 'origin': __file__},
            attachments=[
                self._get_sample_file_path('file_input', 'file_input.wdl'),
                self._get_sample_file_path('file_input', 'test_file_input.txt'),
            ],
        ))

    def test_no_input(self):
        self._run_test(RunRequest(
            workflow_url='hello_world.wdl',
            workflow_params=dict(),
            tags={'origin_lang': 'python', 'origin': __file__},
            attachments=[self._get_sample_file_path('no_input', 'hello_world.wdl')],
        ))

    def test_string_input(self):
        self._run_test(RunRequest(
            workflow_url='hello_name.wdl',
            workflow_params=self._load_json('string_input', 'inputs.json'),
            tags={'origin_lang': 'python', 'origin': __file__},
            attachments=[
                self._get_sample_file_path('string_input', 'hello_name.wdl'),
            ],
        ))

    def _load_json(self, *path):
        file_path = self._get_sample_file_path(*path)
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            # The path is assumed to be an absolute path.
            base_path = self._get_sample_file_path()
            starting_nodes = base_path[1:].split('/')
            final_nodes = file_path[1:].split('/')
            detected_files = []
            for i in range(len(final_nodes) - len(starting_nodes)):
                nodes = starting_nodes + final_nodes[len(starting_nodes): len(starting_nodes) + i]
                node_base_path = '/' + os.path.join(*nodes)
                if os.path.isfile(node_base_path):
                    continue
                detected_files.extend([
                    os.path.join(node_base_path, fn)
                    for fn in os.listdir(node_base_path)
                    if fn[0] != '.'
                ])
            raise RuntimeError(f'File not found at {file_path[len(base_path):]} (exists: {[fn[len(base_path):] for fn in sorted(detected_files)]})')

    def _run_test(self, run_request: RunRequest):
        wes = self.wes
        run_id = wes.submit(run_request)

        self._logger.warning(f'The run ID is {run_id} which you may use it for further inspection.')

        self._wait_until_run_is_listed(wes, run_id)

        run = wes.run(run_id)

        # Wait until it can get the run information (request, state, log URLs, etc.).
        self.wait_until(run.info)

        self._wait_until_run_comes_to_stop(run)

        current_state = run.info().state
        if current_state != 'COMPLETE':
            for log_record in run.get_logs(include_stderr=True):
                self._logger.error(f'{log_record.origin.name}: STDOUT:\n{log_record.stdout}\n')
                self._logger.error(f'{log_record.origin.name}: STDERR:\n{log_record.stderr}\n')
            self.fail(f'The current state of run {run_id} is {current_state}, instead of COMPLETE.')

    def _wait_until_run_is_listed(self, wes: WesClient, run_id: str):
        def assert_that_id_is_listed():
            runs = self._limit_to(wes.get_runs(), 50)
            assert [r for r in runs if r.run_id == run_id], 'Not found'

        self.wait_until(assert_that_id_is_listed)

    def _wait_until_run_comes_to_stop(self, run: Run):
        def assert_completion():
            status = run.status
            assert status in ['COMPLETE', 'EXECUTOR_ERROR'], \
                f'The current state of run {run.id} is unexpectedly "{status}" after a certain time period.'

        self.wait_until(assert_completion, timeout=300)

    @staticmethod
    def _limit_to(iterator: Iterator[T], limit: int) -> List[T]:
        captured = []

        for item in iterator:
            captured.append(item)

            if len(captured) >= limit:
                break

        return captured
