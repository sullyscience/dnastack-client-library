import datetime
import tempfile
from datetime import date

from dnastack.client.workbench.ewes.models import ExtendedRunStatus, ExtendedRun, BatchActionResult, BatchRunResponse, \
    MinimalExtendedRunWithInputs, MinimalExtendedRun, MinimalExtendedRunWithOutputs, ExecutionEngine
from dnastack.alpha.client.workflow.models import Workflow, WorkflowVersion
from .base import WorkbenchCliTestCase


class TestWorkbenchCommand(WorkbenchCliTestCase):
    @staticmethod
    def reuse_session() -> bool:
        return True

    def setUp(self) -> None:
        super().setUp()
        self.invoke('use', f'{self.workbench_base_url}/api/service-registry')
        self.submit_hello_world_workflow_batch()

    def test_runs_list(self):
        runs = self.simple_invoke('workbench', 'runs', 'list')
        self.assert_not_empty(runs, f'Expected at least one run. Found {runs}')

        def test_max_results():
            runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--max-results', 1,
            )
            self.assertEqual(len(runs), 1, f'Expected exactly one run. Found {runs}')
            runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--max-results', 2,
            )
            self.assertEqual(len(runs), 2, f'Expected exactly two runs. Found {runs}')
            runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--max-results', 100,
            )
            self.assertGreater(len(runs), 1, f'Expected at least two runs. Found {runs}')

        test_max_results()

        def test_page_and_page_size():
            first_page_runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--max-results', 1,
                '--page-size', 1,
                '--page', 0,
            )
            self.assertEqual(len(first_page_runs), 1, f'Expected exactly one run. Found {runs}')
            second_page_runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--max-results', 1,
                '--page-size', 1,
                '--page', 1,
            )
            self.assertEqual(len(second_page_runs), 1, f'Expected exactly one run. Found {runs}')
            run_id_on_first_page = ExtendedRunStatus(**first_page_runs[0]).run_id
            run_id_on_second_page = ExtendedRunStatus(**second_page_runs[0]).run_id
            self.assertNotEqual(run_id_on_first_page, run_id_on_second_page,
                                f'Expected two different runs from different pages. '
                                f'Found {run_id_on_first_page} and {run_id_on_second_page}')

        test_page_and_page_size()

        def test_order():
            asc_runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--order', 'start_time ASC',
            )
            self.assertGreater(len(runs), 0, f'Expected at least one run. Found {runs}')
            desc_runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--order', 'start_time DESC',
            )
            self.assertGreater(len(runs), 0, f'Expected at least one run. Found {runs}')
            run_id_from_asc_runs = ExtendedRunStatus(**asc_runs[0]).run_id
            run_id_from_desc_runs = ExtendedRunStatus(**desc_runs[0]).run_id
            self.assertNotEqual(run_id_from_asc_runs, run_id_from_desc_runs,
                                f'Expected two different runs when ordered. '
                                f'Found {run_id_from_asc_runs} and {run_id_from_desc_runs}')

        test_order()

        def test_states():
            runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--state', 'PAUSED',
                '--state', 'UNKNOWN',
            )
            self.assertEqual(len(runs), 0, f'Expected exactly zero runs to be in a given states. Found {runs}')

        test_states()

        def test_submitted_since_and_until():
            today = date.today()
            tomorrow = date.today() + datetime.timedelta(days=1)
            runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--submitted-since', f'{today.year}-{today.month:02d}-{today.day:02d}',
            )
            self.assertGreater(len(runs), 0, f'Expected at least one run. Found {runs}')
            runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--submitted-until', f'{tomorrow.year}-{tomorrow.month:02d}-{tomorrow.day:02d}',
            )
            self.assertGreater(len(runs), 0, f'Expected at least one run. Found {runs}')
            runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--submitted-since', f'{today.year}-{today.month:02d}-{today.day:02d}',
                '--submitted-until', f'{tomorrow.year}-{tomorrow.month:02d}-{tomorrow.day:02d}',
            )
            self.assertGreater(len(runs), 0, f'Expected at least one run. Found {runs}')

        test_submitted_since_and_until()

        def test_engine():
            runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--engine', self.execution_engine.id,
            )
            self.assertGreater(len(runs), 0, f'Expected at least one run. Found {runs}')
            runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--engine', 'unknown-id',
            )
            self.assertEqual(len(runs), 0, f'Expected exactly zero runs. Found {runs}')

        test_engine()

        def test_search():
            runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--max-results', 1,
            )
            run_id = ExtendedRunStatus(**runs[0]).run_id
            searched_runs = self.simple_invoke(
                'workbench', 'runs', 'list',
                '--search', f'{run_id}',
            )
            found_run_id = ExtendedRunStatus(**searched_runs[0]).run_id
            self.assertEqual(len(runs), 1, f'Expected exactly one run. Found {runs}')
            self.assertEqual(found_run_id, run_id, f'Expected runs to be the same. Found {found_run_id}')

        test_search()

    def test_runs_describe(self):
        runs = self.simple_invoke(
            'workbench', 'runs', 'list',
            '--max-results', 2
        )
        self.assertEqual(len(runs), 2, f'Expected exactly two runs. Found {runs}')
        first_run_id = ExtendedRunStatus(**runs[0]).run_id
        second_run_id = ExtendedRunStatus(**runs[1]).run_id

        def test_single_run():
            described_runs = [ExtendedRun(**described_run) for described_run in self.simple_invoke(
                'workbench', 'runs', 'describe',
                first_run_id
            )]
            self.assertEqual(len(described_runs), 1, f'Expected exactly one run. Found {described_runs}')
            self.assertEqual(described_runs[0].run_id, first_run_id, 'Expected to be the same run.')

        test_single_run()

        def test_multiple_runs():
            described_runs = [ExtendedRun(**described_run) for described_run in self.simple_invoke(
                'workbench', 'runs', 'describe',
                first_run_id, second_run_id
            )]
            self.assertEqual(len(described_runs), 2, f'Expected exactly two runs. Found {described_runs}')
            self.assertEqual(described_runs[0].run_id, first_run_id, 'Expected to be the same run.')
            self.assertEqual(described_runs[1].run_id, second_run_id, 'Expected to be the same run.')

        test_multiple_runs()

        def test_status():
            described_runs = [MinimalExtendedRun(**described_run) for described_run in self.simple_invoke(
                'workbench', 'runs', 'describe',
                '--status',
                first_run_id
            )]
            self.assertEqual(len(described_runs), 1, f'Expected exactly one run. Found {described_runs}')

        test_status()

        def test_inputs():
            described_runs = [MinimalExtendedRunWithInputs(**described_run) for described_run in self.simple_invoke(
                'workbench', 'runs', 'describe',
                '--inputs',
                first_run_id
            )]
            self.assertEqual(len(described_runs), 1, f'Expected exactly one run. Found {described_runs}')

        test_inputs()

        def test_outputs():
            described_runs = [MinimalExtendedRunWithOutputs(**described_run) for described_run in self.simple_invoke(
                'workbench', 'runs', 'describe',
                '--outputs',
                first_run_id
            )]
            self.assertEqual(len(described_runs), 1, f'Expected exactly one run. Found {described_runs}')

        test_outputs()

    def test_runs_cancel(self):
        runs = self.simple_invoke(
            'workbench', 'runs', 'list',
            '--max-results', 1
        )
        self.assertEqual(len(runs), 1, f'Expected exactly one run. Found {runs}')
        first_run_id = ExtendedRunStatus(**runs[0]).run_id

        BatchActionResult(**self.simple_invoke(
            'workbench', 'runs', 'cancel',
            first_run_id
        ))

    def test_runs_delete(self):
        runs = self.simple_invoke('workbench', 'runs', 'list')
        self.assertGreater(len(runs), 1, f'Expected at least one run. Found {runs}')
        # We are selecting last run from the list, so we won't delete something other tests depend on
        last_run_id = ExtendedRunStatus(**runs[-1]).run_id

        BatchActionResult(**self.simple_invoke(
            'workbench', 'runs', 'delete',
            '--force',
            last_run_id
        ))

    def test_runs_submit(self):
        def _create_inputs_json_file():
            with tempfile.NamedTemporaryFile(delete=False) as inputs_json_file:
                inputs_json_file.write(b'{"test.hello.name": "bar"}')
                return inputs_json_file.name

        def _create_inputs_text_file():
            with tempfile.NamedTemporaryFile(delete=False) as input_text_fp:
                input_text_fp.write(b'bar')
                return input_text_fp.name

        hello_world_workflow_url = self.get_hello_world_workflow_url()
        input_json_file = _create_inputs_json_file()
        input_text_file = _create_inputs_text_file()

        def test_submit_batch_with_single_key_value_params():
            submitted_batch = BatchRunResponse(**self.simple_invoke(
                'workbench', 'runs', 'submit',
                '--url', hello_world_workflow_url,
                '--workflow-params', 'test.hello.name=foo',
            ))
            self.assertEqual(len(submitted_batch.runs), 1, 'Expected exactly one run to be submitted.')
            described_runs = [MinimalExtendedRunWithInputs(**described_run) for described_run in self.simple_invoke(
                'workbench', 'runs', 'describe',
                '--inputs',
                submitted_batch.runs[0].run_id
            )]
            self.assertEqual(len(described_runs), 1, f'Expected exactly one run. Found {described_runs}')
            self.assertEqual(described_runs[0].inputs, {'test.hello.name': 'foo'},
                             f'Expected workflow params to be exactly the same. Found {described_runs[0].inputs}')

        test_submit_batch_with_single_key_value_params()

        def test_submit_batch_with_single_json_file_params():
            submitted_batch = BatchRunResponse(**self.simple_invoke(
                'workbench', 'runs', 'submit',
                '--url', hello_world_workflow_url,
                '--workflow-params', f'@{input_json_file}',
            ))
            self.assertEqual(len(submitted_batch.runs), 1, 'Expected exactly one run to be submitted.')
            described_runs = [MinimalExtendedRunWithInputs(**described_run) for described_run in self.simple_invoke(
                'workbench', 'runs', 'describe',
                '--inputs',
                submitted_batch.runs[0].run_id
            )]
            self.assertEqual(len(described_runs), 1, f'Expected exactly one run. Found {described_runs}')
            self.assertEqual(described_runs[0].inputs, {'test.hello.name': 'bar'},
                             f'Expected workflow params to be exactly the same. Found {described_runs[0].inputs}')

        test_submit_batch_with_single_json_file_params()

        def test_submit_batch_with_single_inlined_json_params():
            submitted_batch = BatchRunResponse(**self.simple_invoke(
                'workbench', 'runs', 'submit',
                '--url', hello_world_workflow_url,
                '--workflow-params', '{"test.hello.name": "baz"}',
            ))
            self.assertEqual(len(submitted_batch.runs), 1, 'Expected exactly one run to be submitted.')
            described_runs = [MinimalExtendedRunWithInputs(**described_run) for described_run in self.simple_invoke(
                'workbench', 'runs', 'describe',
                '--inputs',
                submitted_batch.runs[0].run_id
            )]
            self.assertEqual(len(described_runs), 1, f'Expected exactly one run. Found {described_runs}')
            self.assertEqual(described_runs[0].inputs, {'test.hello.name': 'baz'},
                             f'Expected workflow params to be exactly the same. Found {described_runs[0].inputs}')

        test_submit_batch_with_single_inlined_json_params()

        def test_submit_batch_with_single_embedded_text_params():
            submitted_batch = BatchRunResponse(**self.simple_invoke(
                'workbench', 'runs', 'submit',
                '--url', hello_world_workflow_url,
                '--workflow-params', f'test.hello.name=@{input_text_file}',
            ))
            self.assertEqual(len(submitted_batch.runs), 1, 'Expected exactly one run to be submitted.')
            described_runs = [MinimalExtendedRunWithInputs(**described_run) for described_run in self.simple_invoke(
                'workbench', 'runs', 'describe',
                '--inputs',
                submitted_batch.runs[0].run_id
            )]
            self.assertEqual(len(described_runs), 1, f'Expected exactly one run. Found {described_runs}')
            self.assertEqual(described_runs[0].inputs, {'test.hello.name': 'bar'},
                             f'Expected workflow params to be exactly the same. Found {described_runs[0].inputs}')

        test_submit_batch_with_single_embedded_text_params()

        def test_submit_batch_with_multiple_params():
            submitted_batch = BatchRunResponse(**self.simple_invoke(
                'workbench', 'runs', 'submit',
                '--url', hello_world_workflow_url,
                '--workflow-params', 'test.hello.name=foo',
                '--workflow-params', f'@{input_json_file}',
                '--workflow-params', '{"test.hello.name": "baz"}',
                '--workflow-params', f'test.hello.name=@{input_text_file}',
            ))
            self.assertEqual(len(submitted_batch.runs), 4, 'Expected exactly three runs to be submitted.')
            described_runs = [MinimalExtendedRunWithInputs(**described_run) for described_run in self.simple_invoke(
                'workbench', 'runs', 'describe',
                '--inputs',
                submitted_batch.runs[0].run_id,
                submitted_batch.runs[1].run_id,
                submitted_batch.runs[2].run_id,
                submitted_batch.runs[3].run_id,
            )]
            self.assertEqual(len(described_runs), 4, f'Expected exactly three runs. Found {described_runs}')
            self.assertEqual(described_runs[0].inputs, {'test.hello.name': 'foo'},
                             f'Expected workflow params to be exactly the same. Found {described_runs[0].inputs}')
            self.assertEqual(described_runs[1].inputs, {'test.hello.name': 'bar'},
                             f'Expected workflow params to be exactly the same. Found {described_runs[1].inputs}')
            self.assertEqual(described_runs[2].inputs, {'test.hello.name': 'baz'},
                             f'Expected workflow params to be exactly the same. Found {described_runs[2].inputs}')
            self.assertEqual(described_runs[3].inputs, {'test.hello.name': 'bar'},
                             f'Expected workflow params to be exactly the same. Found {described_runs[3].inputs}')

        test_submit_batch_with_multiple_params()

    def test_workflows_list(self):
        result = [Workflow(**workflow) for workflow in self.simple_invoke(
            'workbench', 'workflows', 'list'
        )]
        self.assert_not_empty(result, 'Expected at least one workflows.')

        def test_source():
            result = [Workflow(**workflow) for workflow in self.simple_invoke(
                'workbench', 'workflows', 'list',
                '--source', 'DOCKSTORE'
            )]
            self.assertGreater(len(result), 0, 'Expected at least one workflow.')
            self.assertTrue(all(workflow.source == 'DOCKSTORE' for workflow in result),
                            'Expected all workflows to be DOCKSTORE.')

            result = [Workflow(**workflow) for workflow in self.simple_invoke(
                'workbench', 'workflows', 'list',
                '--source', 'PRIVATE'
            )]
            if len(result) == 0:
                self.assertEqual(len(result), 0, 'Expected exactly zero workflows.')
            else:
                self.assertGreater(len(result), 0, 'Expected at least one workflow.')
                self.assertTrue(all(workflow.source == 'PRIVATE' for workflow in result),
                                'Expected all workflows to be PRIVATE.')

        test_source()

    def test_workflows_describe(self):
        result = [Workflow(**workflow) for workflow in self.simple_invoke(
            'workbench', 'workflows', 'list'
        )]
        self.assert_not_empty(result, 'Expected at least one workflows.')
        first_workflow_id = result[0].internalId
        second_workflow_id = result[0].internalId

        def test_single_workflow():
            described_workflow = [Workflow(**described_workflow) for described_workflow in self.simple_invoke(
                'workbench', 'workflows', 'describe',
                first_workflow_id
            )]
            self.assertEqual(len(described_workflow), 1, 'Expected exactly one workflow.')
            self.assertEqual(described_workflow[0].internalId, first_workflow_id, 'Expected to be the same workflow.')

        test_single_workflow()

        def test_multiple_workflows():
            described_workflows = [Workflow(**described_workflow) for described_workflow in self.simple_invoke(
                'workbench', 'workflows', 'describe',
                first_workflow_id, second_workflow_id
            )]
            self.assertEqual(len(described_workflows), 2, 'Expected exactly two workflows.')
            self.assertEqual(described_workflows[0].internalId, first_workflow_id, 'Expected to be the same workflow.')
            self.assertEqual(described_workflows[1].internalId, second_workflow_id, 'Expected to be the same workflow.')

        test_multiple_workflows()

    def test_workflows_create_and_add_version(self):
        def _create_main_wdl_file():
            with open('main.wdl', 'w') as main_wdl_file:
                main_wdl_file.write("""
                version 1.0
                
                workflow no_task_workflow {
                    input {
                        String first_name
                        String? last_name
                    }
                }
                """)

        def _create_description_file():
            with open('description.md', 'w') as description_file:
                description_file.write("""
                TITLE
                DESCRIPTION
                """)

        def _create_workflow() -> Workflow:
            return Workflow(**self.simple_invoke(
                'workbench', 'workflows', 'create',
                'main.wdl',
            ))

        def _create_workflow_version(workflow_id, name) -> WorkflowVersion:
            return WorkflowVersion(**self.simple_invoke(
                'workbench', 'workflows', 'versions', 'create',
                '--workflow', workflow_id,
                '--name', name,
                'main.wdl',
            ))

        _create_main_wdl_file()
        _create_description_file()
        created_workflow = _create_workflow()

        self.assertIsNotNone(created_workflow.internalId, 'Expected custom workflow to be created.')
        self.assertEqual(created_workflow.source, 'PRIVATE', 'Expected workflow to be PRIVATE.')

        def test_name_and_description():
            created_workflow = Workflow(**self.simple_invoke(
                'workbench', 'workflows', 'create',
                '--name', 'foo',
                '--description', '@description.md',
                'main.wdl',
            ))
            self.assertIsNotNone(created_workflow.internalId, 'Expected custom workflow to be created.')
            self.assertEqual(created_workflow.source, 'PRIVATE', 'Expected workflow to be PRIVATE.')
            self.assertEqual(created_workflow.name, 'foo', 'Expected workflow with name "foo".')
            self.assertTrue(
                'TITLE' in created_workflow.description and 'DESCRIPTION' in created_workflow.description,
                'Expected workflow with description')

        test_name_and_description()

        def test_edit_workflow():
            edited_workflow = Workflow(**self.simple_invoke(
                'workbench', 'workflows', 'update',
                '--name', 'UPDATED',
                '--authors', 'foo,bar',
                '--description', 'updated',
                created_workflow.internalId
            ))

            self.assertEqual(edited_workflow.name, 'UPDATED')
            self.assertEqual(edited_workflow.description, 'updated')
            self.assertEqual(edited_workflow.authors, ['foo', 'bar'])

            edited_workflow = Workflow(**self.simple_invoke(
                'workbench', 'workflows', 'update',
                '--description', '@description.md',
                created_workflow.internalId
            ))

            self.assertTrue(
                'TITLE' in edited_workflow.description and 'DESCRIPTION' in edited_workflow.description)

            edited_workflow = Workflow(**self.simple_invoke(
                'workbench', 'workflows', 'update',
                '--name', 'UPDATED',
                '--description', '',
                created_workflow.internalId
            ))

            self.assertIsNone(edited_workflow.description)

        test_edit_workflow()

        def test_add_version():
            created_workflow_version = WorkflowVersion(**self.simple_invoke(
                'workbench', 'workflows', 'versions', 'create',
                '--workflow', created_workflow.internalId,
                '--name', 'foo',
                '--description', '@description.md',
                'main.wdl',
            ))
            self.assertIsNotNone(created_workflow_version.id, 'Expected workflow version ID to be assigned.')
            self.assertEqual(created_workflow_version.versionName, 'foo', 'Expected workflow with name "foo".')
            described_workflows = [Workflow(**described_workflow)
                                   for described_workflow in self.simple_invoke(
                    'workbench', 'workflows', 'describe',
                    created_workflow.internalId
                )]
            self.assertTrue(any(described_workflow_version.id == created_workflow_version.id
                                for described_workflow_version in described_workflows[0].versions),
                            f'Expected new workflow version with ID {created_workflow_version.id}.'
                            f' Workflow versions {described_workflows[0].versions}')

        test_add_version()

        def test_edit_version():
            edited_workflow_version = WorkflowVersion(**self.simple_invoke(
                'workbench', 'workflows', 'versions', 'update',
                '--workflow', created_workflow.internalId,
                '--name', 'UPDATED',
                '--authors', 'foo,bar',
                '--description', 'updated',
                created_workflow.versions[0].id
            ))

            self.assertEqual(edited_workflow_version.versionName, 'UPDATED')
            self.assertEqual(edited_workflow_version.description, 'updated')
            self.assertEqual(edited_workflow_version.authors, ['foo', 'bar'])

            edited_workflow_version = WorkflowVersion(**self.simple_invoke(
                'workbench', 'workflows', 'versions', 'update',
                '--workflow', created_workflow.internalId,
                '--description', '@description.md',
                created_workflow.versions[0].id
            ))

            self.assertTrue(
                'TITLE' in edited_workflow_version.description and 'DESCRIPTION' in edited_workflow_version.description)

            edited_workflow_version = WorkflowVersion(**self.simple_invoke(
                'workbench', 'workflows', 'versions', 'update',
                '--workflow', created_workflow.internalId,
                '--description', '',
                '--authors', '',
                created_workflow.versions[0].id
            ))

            self.assertIsNone(edited_workflow_version.description)
            self.assertEqual(edited_workflow_version.authors, [])

        test_edit_version()

        def test_delete_version():
            version_to_delete = _create_workflow_version(created_workflow.internalId, "to-delete")
            output = self.simple_invoke(
                'workbench', 'workflows', 'versions', 'delete',
                '--force', '--workflow', created_workflow.internalId,
                version_to_delete.id,
                parse_output=False
            )

            self.assertTrue("Deleted..." in output)

            versions = [WorkflowVersion(**workflow_version) for workflow_version in self.simple_invoke(
                'workbench', 'workflows', 'versions', 'list',
                '--workflow', created_workflow.internalId,
            )]

            self.assert_not_empty(versions)
            self.assertTrue(version_to_delete.id not in [version.id for version in versions])

            versions = [WorkflowVersion(**workflow_version) for workflow_version in self.simple_invoke(
                'workbench', 'workflows', 'versions', 'list',
                '--workflow', created_workflow.internalId,
                '--include-deleted'
            )]

            self.assert_not_empty(versions)
            self.assertTrue(version_to_delete.id in [version.id for version in versions])

            result = self.invoke(
                'workbench', 'workflows', 'versions', 'describe',
                '--workflow', created_workflow.internalId,
                version_to_delete.id,
                bypass_error=True
            )

            self.assertNotEqual(result.exit_code, 0)
            self.assertTrue('"error_code":404' in result.stderr)

            version = WorkflowVersion(**self.simple_invoke(
                'workbench', 'workflows', 'versions', 'describe',
                '--workflow', created_workflow.internalId,
                '--include-deleted',
                version_to_delete.id
            )[0])

            self.assertTrue(version.deleted)
            self.assertEqual(version.id, version_to_delete.id)

        test_delete_version()

        def test_delete_workflow():
            workflow_to_delete = _create_workflow()
            output = self.simple_invoke(
                'workbench', 'workflows', 'delete',
                '--force', workflow_to_delete.internalId,
                parse_output=False
            )

            self.assertTrue("Deleted..." in output)

            workflows = [Workflow(**workflow) for workflow in self.simple_invoke(
                'workbench', 'workflows', 'list'
            )]

            self.assert_not_empty(workflows)
            self.assertTrue(all(workflow_to_delete.internalId != workflow.internalId for workflow in workflows))

            workflows = [Workflow(**workflow) for workflow in self.simple_invoke(
                'workbench', 'workflows', 'list',
                '--include-deleted'
            )]

            self.assert_not_empty(workflows)
            self.assertTrue(any(workflow_to_delete.internalId == workflow.internalId for workflow in workflows))

            result = self.invoke(
                'workbench', 'workflows', 'describe',
                workflow_to_delete.internalId,
                bypass_error=True
            )

            self.assertNotEqual(result.exit_code, 0)
            self.assertTrue('"error_code":404' in result.stderr)

            workflow = Workflow(**self.simple_invoke(
                'workbench', 'workflows', 'describe',
                '--include-deleted',
                workflow_to_delete.internalId,
            )[0])

            self.assertTrue(workflow.deleted)
            self.assertEqual(workflow.internalId, workflow_to_delete.internalId)

        test_delete_workflow()

    def test_workflow_version_list(self):
        workflow_result = [Workflow(**workflow) for workflow in self.simple_invoke(
            'workbench', 'workflows', 'list'
        )]
        self.assert_not_empty(workflow_result, 'Expected at least one workflows.')
        workflow_id = workflow_result[0].internalId

        result = [WorkflowVersion(**workflow_version) for workflow_version in self.simple_invoke(
            'workbench', 'workflows', 'versions', 'list', '--workflow', workflow_id
        )]
        self.assert_not_empty(result, f'Expected at least one workflows version in workflow {workflow_id}')

    def test_workflow_version_describe(self):
        workflow_result = [Workflow(**workflow) for workflow in self.simple_invoke(
            'workbench', 'workflows', 'list'
        )]
        self.assert_not_empty(workflow_result, 'Expected at least one workflows.')
        workflow_id = workflow_result[0].internalId

        workflow = [Workflow(**w) for w in self.simple_invoke(
            'workbench', 'workflows', 'describe', workflow_id
        )][0]

        result = [WorkflowVersion(**workflow_version) for workflow_version in self.simple_invoke(
            'workbench', 'workflows', 'versions', 'describe', '--workflow', workflow_id,
            workflow.versions[0].id
        )]
        self.assertTrue(len(result) == 1, "Expected the number of workflow versions to be exactly 1")
        self.assertEqual(result[0].id, workflow.versions[0].id)

    def test_engine_list(self):
        engines_result = [ExecutionEngine(**engine) for engine in self.simple_invoke(
            'alpha', 'workbench', 'engines', 'list'
        )]

        self.assert_not_empty(engines_result, "Expected at least one engine")
        self.assertTrue(any(engine.id == self.execution_engine.id for engine in engines_result))

    def test_engine_describe(self):
        engines_result = [ExecutionEngine(**engine) for engine in self.simple_invoke(
            'alpha', 'workbench', 'engines', 'describe', self.execution_engine.id
        )]

        self.assert_not_empty(engines_result, "Expected engine result to not be empty")
        self.assertEqual(len(engines_result), 1, "Expected only one engine")
        self.assertTrue(any(engine.id == self.execution_engine.id for engine in engines_result))
