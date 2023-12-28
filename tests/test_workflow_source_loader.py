import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Optional, Union

from dnastack.alpha.client.workflow.models import WorkflowFile, WorkflowFileType
from dnastack.alpha.client.workflow.utils import WorkflowSourceLoader, WorkflowSourceLoaderError


class TestWorkflowFile(WorkflowFile):
    test_file_path: Optional[Union[Path, str]]


LIBRARY_FILES = [
    WorkflowFile(path="subworkflow.wdl", content="version 1.0\n", file_type=WorkflowFileType.secondary),
    WorkflowFile(path="workflows/wgs/subworkflow.wdl", content="version 1.0\n"
                 , file_type=WorkflowFileType.secondary),
    WorkflowFile(path="utilities/structs/wgs_structs.wdl", content="version 1.0\n",
                 file_type=WorkflowFileType.secondary),
    WorkflowFile(path="utilities/tasks/helpers.wdl", content="version 1.0\n"
                 , file_type=WorkflowFileType.secondary),
    WorkflowFile(path="tasks/wgs/quality.wdl", content="version 1.0\n"
                 , file_type=WorkflowFileType.secondary),
    WorkflowFile(path="tasks/wgs/align-and-call.wdl", content="version 1.0\n"
                 , file_type=WorkflowFileType.secondary),
]

SUCCESS_WORKFLOW = WorkflowFile(path="main.wdl", file_type=WorkflowFileType.primary, content=""""
version 1.0
import 'utilities/structs/wgs_structs.wdl'
import 'utilities/tasks/helpers.wdl'
import 'tasks/wgs/align-and-call.wdl'
import 'tasks/wgs/quality.wdl'
import 'subworkflow.wdl',
import 'workflows/wgs/subworkflow.wdl'
""")

SUCCESS_WORKFLOW_NESTED = WorkflowFile(path="workflows/wgs/main2.wdl",
                                       file_type=WorkflowFileType.primary,
                                       content=""""
version 1.0
import '../../utilities/structs/wgs_structs.wdl'
import '../../utilities/tasks/helpers.wdl'
import '../../tasks/wgs/align-and-call.wdl'
import '../../tasks/wgs/quality.wdl'
import '../../subworkflow.wdl'
import 'subworkflow.wdl'
""")

SUCCESS_NO_IMPORT_WORKFLOW = WorkflowFile(path="no-import.wdl", content="version 1.0\n",
                                          file_type=WorkflowFileType.secondary)

SUCCESS_NO_IMPORT_WORKFLOW_NESTED = WorkflowFile(path="workflows/wgs/no-import.wdl", content="version 1.0\n",
                                                 file_type=WorkflowFileType.secondary)

UNKNOWN_IMPORT = WorkflowFile(path="unknown_import.wdl", file_type=WorkflowFileType.primary, content=""""
version 1.0
import 'unkown.wdl'
""")

UNKNOWN_IMPORT_NESTED = WorkflowFile(path="workflows/wgs/unknown_import.wdl", file_type=WorkflowFileType.primary,
                                     content=""""
version 1.0
import 'unkown.wdl'
""")


class WorkflowSourceLoaderTestcase(unittest.TestCase):
    def setUp(self):
        self.tempdir = TemporaryDirectory()

    def tearDown(self):
        self.tempdir.cleanup()

    def create_files(self, workflow_files: List[WorkflowFile]) -> List[TestWorkflowFile]:
        test_files: List[TestWorkflowFile] = list()
        for workflow_to_write in workflow_files:
            path = Path(f"{self.tempdir.name}/{workflow_to_write.path}")
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as fd:
                fd.write(workflow_to_write.content)
                test_files.append(TestWorkflowFile(**workflow_to_write.__dict__, test_file_path=path))

        self.assertEqual(len(test_files), len(workflow_files), f"Expecting {len(workflow_files)} files to be created")
        self.assertTrue(all(str(test_file.test_file_path).startswith(self.tempdir.name) for test_file in test_files))
        return test_files

    def test_constructor_requires_at_least_one_file(self):
        self.assertRaises(TypeError, WorkflowSourceLoader)
        self.assertRaises(WorkflowSourceLoaderError, WorkflowSourceLoader, [])

    def test_non_existent_file_throws_ioexception(self):
        self.assertRaises(IOError, WorkflowSourceLoader, ["foo/bar/1919282.wdl"])

    def test_no_import_workflow_loads(self):
        test_file = self.create_files(workflow_files=[SUCCESS_NO_IMPORT_WORKFLOW])[0]
        loader = WorkflowSourceLoader([test_file.test_file_path])
        self.assertEqual(len(loader.loaded_files), 1)
        self.assertIn(self.tempdir.name, str(test_file.test_file_path),
                      "Expect test files to be created in temp dir")
        self.assertNotIn(self.tempdir.name, loader.loaded_files[0].path,
                         "Expect Computed paths to not contain temp dir")
        self.assertEqual(loader.loaded_files[0].path, SUCCESS_NO_IMPORT_WORKFLOW.path,
                         "Expecting the computed path to be the same as the No Import workflow path")

    def test_nested_no_import_worfklow_loads_and_removes_leading_path(self):
        test_file = self.create_files(workflow_files=[SUCCESS_NO_IMPORT_WORKFLOW_NESTED])[0]
        loader = WorkflowSourceLoader([test_file.test_file_path])
        self.assertEqual(len(loader.loaded_files), 1)
        self.assertNotIn(self.tempdir.name, loader.loaded_files[0].path,
                         "Expect the computed paths to not contain temp dir")
        self.assertNotEquals(loader.loaded_files[0].path, SUCCESS_NO_IMPORT_WORKFLOW_NESTED.path,
                             "Expecting the computed path to not be the same as the No Import workflow path")
        self.assertEquals(loader.loaded_files[0].path, Path(SUCCESS_NO_IMPORT_WORKFLOW_NESTED.path).name,
                          "Expecting the compute path to be the same as the No Import workflow path")

    def test_un_nested_workflow_loads_with_all_imports(self):
        workflow_files = [SUCCESS_WORKFLOW] + LIBRARY_FILES
        files_has_path = lambda x: any(workflow_file.path == x for workflow_file in workflow_files)

        test_files = self.create_files(workflow_files=workflow_files)
        # Only pass in the first workflow, allow the auto discover to find the rest of them
        loader = WorkflowSourceLoader([test_files[0].test_file_path])
        self.assertEqual(len(loader.loaded_files), len(workflow_files))
        self.assertTrue(all(files_has_path(loaded_file.path) for loaded_file in loader.loaded_files),
                        "Expecting all of the loaded files paths to match their original relative paths")

    def test_nested_workflow_loads_with_all_imports(self):
        workflow_files = [SUCCESS_WORKFLOW_NESTED] + LIBRARY_FILES
        files_has_path = lambda x: any(workflow_file.path == x for workflow_file in workflow_files)
        test_files = self.create_files(workflow_files=workflow_files)
        original_directory = Path(os.curdir).absolute()
        try:
            # Change directory so that the other workflows are not in the current directory
            # These have to be resolved outside the scope of the current directory and will
            # force the WorkflowSourceLoader to append all unique attributes to the main workflow path
            os.chdir(test_files[0].test_file_path.parent)
            self.assertNotEqual(original_directory, Path(os.curdir).absolute())
            # Only pass in the first workflow, allow the auto discover to find the rest of them
            loader = WorkflowSourceLoader([test_files[0].test_file_path.name])

            self.assertEqual(len(loader.loaded_files), len(workflow_files))
            self.assertNotEqual(loader.loaded_files[0].path, test_files[0].test_file_path.name)
            self.assertTrue(all(files_has_path(loaded_file.path) for loaded_file in loader.loaded_files),
                            "Expecting all of the loaded files paths to match their original relative paths")
        finally:
            os.chdir(original_directory)

    def test_importing_non_existent_file_fails(self):
        workflow_files = [UNKNOWN_IMPORT]
        test_file = self.create_files(workflow_files=workflow_files)[0]
        self.assertRaises(IOError,WorkflowSourceLoader,[test_file.path])

    def test_importing_non_existent_nested_file_fails(self):
        workflow_files = [UNKNOWN_IMPORT_NESTED]
        test_file = self.create_files(workflow_files=workflow_files)[0]
        self.assertRaises(IOError, WorkflowSourceLoader, [test_file.path])