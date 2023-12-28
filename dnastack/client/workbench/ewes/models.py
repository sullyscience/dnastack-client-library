from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
import re

from pydantic import BaseModel

from dnastack.client.workbench.models import BaseListOptions, PaginatedResource
from dnastack.client.service_registry.models import Service
from dnastack.common.json_argument_parser import JSONType


class Outcome(str, Enum):
    SUCCESS = 'SUCCESS',
    FAILURE = 'FAILURE'


class LogType(str, Enum):
    STDOUT = 'stdout',
    STDERR = 'stderr',


class State(str, Enum):
    UNKNOWN = "UNKNOWN"
    QUEUED = "QUEUED"
    INITIALIZING = "INITIALIZING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    CANCELING = "CANCELING"
    COMPLETE = "COMPLETE"
    EXECUTOR_ERROR = "EXECUTOR_ERROR"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    CANCELED = "CANCELED"
    COMPLETE_WITH_ERRORS = "COMPLETE_WITH_ERRORS"

    def is_error(self) -> bool:
        return self in [State.COMPLETE_WITH_ERRORS, State.EXECUTOR_ERROR, State.SYSTEM_ERROR]

    def is_terminal(self) -> bool:
        return self in [State.COMPLETE, State.COMPLETE_WITH_ERRORS, State.CANCELED, State.EXECUTOR_ERROR,
                        State.SYSTEM_ERROR]


class WesServiceInfo(Service):
    workflow_type_versions: Optional[Dict]
    supported_wes_versions: Optional[List[str]]
    supported_filesystem_protocols: Optional[List[str]]
    workflow_engine_versions: Optional[Dict]
    default_workflow_engine_parameters: Optional[List[Dict]]
    system_state_counts: Optional[Dict]
    auth_instructions_url: Optional[str]
    tags: Optional[Dict]


class ExtendedRunStatus(BaseModel):
    run_id: str
    external_id: Optional[str]
    state: State
    start_time: datetime
    end_time: Optional[datetime]
    submitted_by: Optional[str]
    workflow_id: Optional[str]
    workflow_version_id: Optional[str]
    workflow_url: Optional[str]
    workflow_name: Optional[str]
    workflow_version: Optional[str]
    workflow_authors: Optional[List[str]]
    workflow_type: Optional[str]
    workflow_type_version: Optional[str]
    workflow_params: Optional[Dict]
    tags: Optional[Dict]
    workflow_engine_parameters: Optional[Dict]


class Log(BaseModel):
    task_id: Optional[str]
    name: str
    pretty_name: Optional[str]
    cmd: Optional[Any]
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    stdout: Optional[str]
    stderr: Optional[str]
    exit_code: Optional[int]
    state: Optional[State]


class ExtendedRunRequest(BaseModel):
    workflow_url: Optional[str]
    workflow_name: Optional[str]
    workflow_version: Optional[str]
    workflow_authors: Optional[List[str]]
    workflow_type: Optional[str]
    workflow_type_version: Optional[str]
    workflow_id: Optional[str]
    workflow_version_id: Optional[str]
    submitted_by: Optional[str]
    workflow_params: Optional[Dict]
    workflow_engine_parameters: Optional[Dict]
    tags: Optional[Dict]


class ExtendedRun(BaseModel):
    run_id: str
    external_id: Optional[str]
    engine_id: Optional[str]
    request: Optional[ExtendedRunRequest]
    state: Optional[State]
    run_log: Optional[Log]
    errors: Optional[List[str]]
    task_logs: Optional[List[Log]]
    task_logs_url: Optional[str]
    outputs: Optional[Dict]


class MinimalExtendedRun(BaseModel):
    run_id: Optional[str]
    state: Optional[State]
    msg: Optional[str]
    error_code: Optional[int]
    timestamp: Optional[str]
    trace_id: Optional[str]



class MinimalExtendedRunWithInputs(BaseModel):
    run_id: str
    inputs: Optional[Dict]


class MinimalExtendedRunWithOutputs(BaseModel):
    run_id: str
    outputs: Optional[Dict]


class BatchRunRequest(BaseModel):
    workflow_url: str
    workflow_type: Optional[str]
    workflow_type_version: Optional[str]
    engine_id: Optional[str]
    default_workflow_params: Optional[Dict]
    default_workflow_engine_parameters: Optional[Dict]
    default_tags: Optional[Dict]
    run_requests: Optional[List[ExtendedRunRequest]]


class BatchRunResponse(BaseModel):
    runs: List[MinimalExtendedRun]


class RunId(BaseModel):
    run_id: str
    state: Optional[State]


class WorkbenchApiError(BaseModel):
    timestamp: Optional[str]
    msg: Optional[str]
    error_code: Optional[int]
    trace_id: Optional[str]


class ActionResult(BaseModel):
    outcome: Outcome
    data: Optional[Any]
    exception: Optional[WorkbenchApiError]


class BatchActionResult(BaseModel):
    results: List[ActionResult]


class TaskListResponse(PaginatedResource):
    tasks: List[Log]

    def items(self) -> List[Any]:
        return self.tasks


class ExtendedRunListResponse(PaginatedResource):
    runs: List[ExtendedRunStatus]

    def items(self) -> List[Any]:
        return self.runs


class ExtendedRunListOptions(BaseListOptions):
    expand: Optional[bool]
    until: Optional[str]
    since: Optional[str]
    search: Optional[str]
    order: Optional[str]
    direction: Optional[str]
    batch_id: Optional[str]
    state: Optional[List[State]]
    engine_id: Optional[str]
    submitted_by: Optional[str]
    workflow_name: Optional[str]
    workflow_version: Optional[str]
    workflow_url: Optional[str]
    workflow_type: Optional[str]
    workflow_type_version: Optional[str]
    tag: Optional[List[str]]


class TaskListOptions(BaseListOptions):
    pass


class ExecutionEngineProviderType(str, Enum):
    AWS = "AWS"
    AZURE = "AZURE"
    GCP = "GCP"
    LOCAL = "LOCAL"


class ExecutionEngine(BaseModel):
    id: str
    internal_id: str
    name: str
    description: Optional[str]
    provider: ExecutionEngineProviderType
    region: Optional[str]
    default: Optional[bool]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    health: Optional[str]
    engine_adapter_configuration: Optional[Dict[str, JSONType]]


class ExecutionEngineListResponse(PaginatedResource):
    engines: List[ExecutionEngine]

    def items(self) -> List[ExecutionEngine]:
        return self.engines


class ExecutionEngineListOptions(BaseListOptions):
    pass
