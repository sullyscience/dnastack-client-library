from enum import Enum
from typing import Dict, Optional, Any, List

from pydantic import BaseModel, Field

from dnastack.client.workbench.models import BaseListOptions, PaginatedResource


class WorkflowDescriptor(BaseModel):
    workflow_name: str
    input_schema: Dict
    output_schema: Dict
    wdl_version: str
    errors: Optional[Any]


class WorkflowVersion(BaseModel):
    workflowId: Optional[str]
    id: str
    externalId: Optional[str]
    versionName: str
    workflowName: str
    createdAt: Optional[str]
    lastUpdatedAt: Optional[str]
    descriptorType: str
    authors: Optional[List[str]] = None
    description: Optional[str] = None
    deleted: Optional[bool] = None
    etag: Optional[str] = None


class Workflow(BaseModel):
    internalId: str
    source: str
    name: str
    description: Optional[str]
    lastUpdatedAt: Optional[str]
    latestVersion: str
    authors: Optional[List[str]]
    versions: Optional[List[WorkflowVersion]]
    deleted: Optional[bool]
    etag: Optional[str]


class WorkflowFileType(str, Enum):
    primary = "PRIMARY_DESCRIPTOR"
    secondary = "DESCRIPTOR"
    test_file = "TEST_FILE"
    other = "OTHER"


class WorkflowSource(str, Enum):
    dockstore = "DOCKSTORE"
    custom = "CUSTOM"
    private = "PRIVATE"


class WorkflowFile(BaseModel):
    path: str
    file_type: WorkflowFileType
    content: Optional[str] = None
    file_url: Optional[str] = None


class WorkflowCreate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    versionName: Optional[str] = None
    files: List[WorkflowFile]


class WorkflowVersionCreate(BaseModel):
    versionName: str
    descriptions: Optional[str]
    files: List[WorkflowFile]


class WorkflowListOptions(BaseListOptions):
    search: Optional[str]
    source: Optional[WorkflowSource]
    deleted: Optional[bool]
    order: Optional[str]
    direction: Optional[str]


class WorkflowListResponse(PaginatedResource):
    workflows: List[Workflow]

    def items(self) -> List[Any]:
        return self.workflows

class WorkflowVersionListOptions(BaseListOptions):
    deleted: Optional[bool]

class WorkflowVersionListResponse(PaginatedResource):
    versions: List[WorkflowVersion]

    def items(self) -> List[Any]:
        return self.versions
