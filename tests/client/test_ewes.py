from typing import TypeVar

from dnastack.client.workbench.ewes.client import EWesClient
from dnastack.client.workbench.ewes.models import ExtendedRunListOptions
from dnastack.common.environments import flag
from tests.exam_helper_for_workbench import BaseWorkbenchTestCase

T = TypeVar('T')


class TestClient(BaseWorkbenchTestCase):
    _ewes_tests_enabled = flag('E2E_EWES_TESTS_ENABLED')

    def setUp(self):
        super(TestClient, self).setUp()

        if not self._ewes_tests_enabled:
            self.skipTest('Tests disabled... Set E2E_EWES_TESTS_ENABLED to true to enable.')

    def test_submit_run(self):
        submitted_run = self.submit_hello_world_workflow_run()
        expected_states = ['QUEUED', 'INITIALIZING', 'RUNNING']
        self.assertTrue(submitted_run.state in expected_states, f'Expected any state from {expected_states}. '
                                                                f'Instead found {submitted_run.state}.')

    def test_submit_batch(self):
        submitted_batch = self.submit_hello_world_workflow_batch()
        expected_states = ['QUEUED', 'INITIALIZING', 'RUNNING']
        self.assertEqual(len(submitted_batch.runs), 2, 'Expected exactly two runs in a batch.')
        self.assertTrue(submitted_batch.runs[0].state in expected_states,
                        f'Expected any state from {expected_states}. Instead found {submitted_batch.runs[0].state}.')

    def test_get_run(self):
        ewes_client: EWesClient = self.get_ewes_client()
        submitted_run = self.submit_hello_world_workflow_run()

        run = ewes_client.get_run(submitted_run.run_id)
        self.assertEqual(run.run_id, submitted_run.run_id, 'Expected IDs to be same.')
        self.assertEqual(run.request.workflow_params.get('test.hello.name'), 'foo',
                         'Expected workflow param to be equal to \'foo\'.')

    def test_list_runs(self):
        ewes_client: EWesClient = self.get_ewes_client()
        self.submit_hello_world_workflow_batch()

        runs = list(ewes_client.list_runs(list_options=None, max_results=None))
        self.assertGreater(len(runs), 1, 'Expected at least two runs.')

        runs = list(ewes_client.list_runs(list_options=None, max_results=1))
        self.assertEqual(len(runs), 1, 'Expected exactly one run.')

        runs = list(ewes_client.list_runs(list_options=ExtendedRunListOptions(engine_id=self.execution_engine.id),
                                          max_results=None))
        self.assertGreater(len(runs), 1, 'Expected at least two runs.')

        runs = list(ewes_client.list_runs(list_options=ExtendedRunListOptions(engine_id='foo'),
                                          max_results=None))
        self.assertEqual(len(runs), 0, 'Expected exactly zero runs.')
