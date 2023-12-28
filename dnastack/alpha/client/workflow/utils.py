import os
from pathlib import Path
from typing import List, Tuple, Optional
import re

from dnastack.alpha.client.workflow.models import WorkflowFile, WorkflowFileType


class WorkflowSourceLoaderError(Exception):
    pass


class WorkflowSourceLoader(object):
    def __init__(self, source_files: List[str]):
        if not source_files:
            raise WorkflowSourceLoaderError("Cannot load workflow source files. At least one file must be specified")
        self.source_files = source_files
        self._load_files()

    def _resolve_content(self, uri: Optional[Path], path: Path, _visited: List[Path]) -> List[Tuple[Path, str]]:
        cwd = Path.cwd().resolve()
        try:
            if not uri:
                import_path = path.resolve()
            else:
                os.chdir(uri)
                import_path = Path(os.path.join(uri, path)).resolve()

            if not import_path.exists():
                raise IOError(
                    f"Could not open file {import_path}. Caller does not have permission or it does not not exist or is")

            if import_path not in _visited:
                _visited.append(import_path)
            else:
                return list()

            pattern = re.compile(r'^\s*import\s["\'](?!http)(.+)["\'].*$')
            resolved_content = list()
            all_imported_contents = list()
            with import_path.open() as fp:
                content = ""
                for line in fp.readlines():
                    content += line
                    match = pattern.match(line)
                    if match:
                        import_statement = Path(match.group(1))
                        imported_contents = self._resolve_content(import_path.parent, import_statement, _visited)
                        all_imported_contents.extend(imported_contents)
                resolved_content.append((import_path, content))
                resolved_content.extend(all_imported_contents)
            return resolved_content
        finally:
            os.chdir(cwd)

    def _load_files(self):
        path_and_contents: List[Tuple[Path, str]] = list()
        _visited = list()
        for path in self.source_files:
            path_and_contents.extend(self._resolve_content(None, Path(path), _visited))
        wdl_files = [wdl_file for wdl_file in path_and_contents if wdl_file[0].name.endswith(".wdl")]
        if len(wdl_files) == 0:
            raise ValueError("No WDL files defined")
        primary_path = wdl_files[0][0]
        primary_set = False
        files = list()
        for (file, content) in path_and_contents:
            if file.name.endswith(".wdl"):
                if primary_set:
                    file_type = WorkflowFileType.secondary
                else:
                    file_type = WorkflowFileType.primary
                    primary_set = True
            elif file.name.endswith(".json"):
                file_type = WorkflowFileType.test_file
            else:
                file_type = WorkflowFileType.other

            files.append(WorkflowFile(
                path=str(file),
                file_type=file_type,
                content=content
            ))

        # Add the parent of the primary path
        # Given the list of absolute paths, strip the common leading path off of the set of paths
        # and the relativize each file path to the common leading path
        common_path = Path(os.path.commonpath([file.path for file in files] + [primary_path.parent]))
        for file in files:
            file.path = str(Path(file.path).relative_to(common_path))
        self._loaded_files = files

    @property
    def loaded_files(self) -> List[WorkflowFile]:
        return self._loaded_files
