import asyncio
import json
import datetime

import random
import string
from enum import Enum, unique
from typing import Optional, List, Iterator, Callable, Iterable, Union

import time
import dnastack

#from dnastack.alpha.client.workflow.client import WorkflowClient
#from dnastack.alpha.client.workflow.models import Workflow, WorkflowVersion
from dnastack.client.workbench.workflow.client import WorkflowClient
from dnastack.client.workbench.workflow.models import Workflow, WorkflowVersion, WorkflowSource, WorkflowListOptions

from dnastack.client.workbench.ewes.client import EWesClient
from dnastack.client.workbench.ewes.models import ExtendedRunListOptions, ExtendedRunStatus, RunId, ExtendedRun, \
	ExtendedRunRequest, LogType, Log, BatchRunRequest
from dnastack.common.logger import get_logger


class WorkbenchBatchException(Exception):
	def __init__(self, message: str, failed_batches: List[str]):
		super().__init__(message)

		self._failed_batches = failed_batches

	@property
	def failed_batches(self)->List[str]:
		return self._failed_batches

class WorkbenchRunException(Exception):
	def __init__(self, message: str, run_ids:Optional[List[str]]):
		super().__init__(message)
		self._failed_runs = run_ids

	@property
	def failed_runs(self)->List[str]:
		return self._failed_runs

class WorkbenchCancellationException(Exception):
	def __init__(self, message: str, run_ids:List[str]):
		super().__init__(message)
		self._run_ids = run_ids

	@property
	def run_ids(self) -> List[str]:
		return self._run_ids

@unique
class RunStatus(Enum):
	# The state of the task is unknown. This provides a safe default for messages where this field is missing, for example, so that a missing field does not accidentally imply that the state is QUEUED.
	UNKNOWN = "UNKNOWN"

	# The workflow is queued.
	QUEUED = "QUEUED"

	# The task has been assigned to a worker and is currently preparing to run. For example, the worker may be turning on, downloading input files, etc.
	INITIALIZING = "INITIALIZING"

	# The task is running. Input files are downloaded and the first Executor has been started.
	RUNNING = "RUNNING"

	# The task is paused. An implementation may have the ability to pause a task, but this is not required.
	PAUSED = "PAUSED"

	# The task has completed running. Executors have exited without error and output files have been successfully uploaded.
	COMPLETE = "COMPLETE"

	# The task encountered an error in one of the Executor processes. Generally, this means that an Executor exited with a non-zero exit code.
	EXECUTOR_ERROR = "EXECUTOR_ERROR"

	# The task was stopped due to a system error, but not from an Executor, for example an upload failed due to network issues, the worker's ran out of disk space, etc.
	SYSTEM_ERROR = "SYSTEM_ERROR"

	# The task was canceled by the user.
	CANCELED = "CANCELED"

	# The task was canceled by the user, and is in the process of stopping.
	CANCELING = "CANCELING"

	# returns true if the task has failed
	def has_failed(self):
		return self == RunStatus.EXECUTOR_ERROR or self == RunStatus.SYSTEM_ERROR

	def was_canceled(self):
		return self == RunStatus.CANCELED or self == RunStatus.CANCELING

class Workbench:
	__CUSTOM_WORKFLOW_SOURCE_NAME = WorkflowSource.private
	__WORKFLOW_MAX_RESULTS = 1000
	def __init__(self, workbench_url:str):
		self._workbench_url = workbench_url
		self._ewes_client_factory = dnastack.use(workbench_url)
		self._workflow_client = None
		self._ewes_client = None
		self._logger = get_logger(type(self).__name__)

	def _get_short_uuid(self) -> str:
		alphabet = string.ascii_lowercase + string.digits
		return ''.join(random.choices(alphabet, k=10))

	def _get_workflow_client(self) -> WorkflowClient:
		if self._workflow_client is None:
			for endpoint in self._ewes_client_factory.all():
				if endpoint.id == 'workflow-service':
					self._workflow_client = WorkflowClient.make(endpoint, namespace=None)
					return self._workflow_client
			raise Exception("Unable to find workflow client")
		else:
			return self._workflow_client

	def _get_ewes_client(self) -> EWesClient:
		if self._ewes_client is None:
			for endpoint in self._ewes_client_factory.all():
				if endpoint.id == 'ewes-service':
					self._ewes_client = EWesClient.make(endpoint, namespace=None)
					return self._ewes_client
			raise Exception("Unable to find workflow client")
		else:
			return self._ewes_client


	# Returns the workflow with the given name.
	def get_workflow(self, workflow_name: str, workflow_type: Optional[WorkflowSource] = __CUSTOM_WORKFLOW_SOURCE_NAME, max_results: Optional[int] = __WORKFLOW_MAX_RESULTS) -> Workflow:
		for workflow in self._get_workflow_client().list_workflows(list_options=WorkflowListOptions(source=workflow_type), max_results=max_results):
			if workflow.name == workflow_name:
				return self._get_workflow_client().get_workflow(workflow.internalId)

		raise Exception("Unable to find workflow " + workflow_name)

	def list_runs(self,
				  list_options: Optional[ExtendedRunListOptions] = None,
				  max_results: int = None) -> Iterator[ExtendedRunStatus]:
		return self._get_ewes_client().list_runs(list_options, max_results)

	def get_status(self, run_id: str) -> RunStatus:
		return RunStatus(self._get_ewes_client().get_status(run_id).state)

	def describe_run(self, run_id: str, include_tasks: bool = True) -> ExtendedRun:
		return self._get_ewes_client().get_run(run_id, include_tasks)

	def describe_batch(self, batch_id: str) -> Iterator[ExtendedRunStatus]:
		return self._get_ewes_client().list_runs(list_options=ExtendedRunListOptions(tag=[f"batch_id:{batch_id}"]))

	def submit_batch(self, batch: BatchRunRequest, batch_id:Optional[str]=None) -> str:
		self._logger.debug("Submitting batch request: "+json.dumps(batch.dict()))
		if batch_id is None:
			batch_id = self._get_short_uuid()

		if batch.default_tags is None:
			batch.default_tags =  {'batch_id':batch_id}
		else:
			batch.default_tags['batch_id'] = batch_id

		self._get_ewes_client().submit_batch(batch)

		return batch_id

	def submit_run(self, data: ExtendedRunRequest) -> str:
		result = self._get_ewes_client().submit_run(data)
		if RunStatus(result.state).has_failed():
			raise WorkbenchRunException("Could not submit run, unexpected run state '"+result.state)
		return result.run_id

	def cancel_batch(self, batch_id: str) -> RunStatus:
		bad_run_ids=[]
		for run_status in self.describe_batch(batch_id):
			r=RunStatus(run_status.state)
			if not r.has_failed() and not r.was_canceled():
				try:
					self.cancel_run(run_status.run_id)
				except Exception as rex:
					bad_run_ids.append(run_status.run_id)
		if(len(bad_run_ids) > 0):
			raise WorkbenchRunException("Could not cancel all runs in batch "+batch_id+".  Run IDs: "+"\n".join(bad_run_ids), run_ids=bad_run_ids)

	def cancel_run(self, run_id) -> RunStatus:
		result = self._get_ewes_client().cancel_run(run_id)
		if type(result) is RunId:
			return RunStatus(RunId.state)
		else:
			raise WorkbenchRunException("Run "+run_id+" could not be canceled: "+json.dumps(result.dict()))

	# Returns the state that all of the runs are in unless:
	# - If any run has a failed status, then that status is returned, otherwise
	# - If any run has a canceled status, then that status is returned, otherwise
	# - If the runs are not all in the same state, then RunStatus.Unknown is returned.
	def _get_unanimous_state(self, batch_id:str) -> RunStatus:
		batch_error_message = ""
		current_unanimous_state = None
		runs_in_batch = self.describe_batch(batch_id)
		for run in runs_in_batch:
			this_runs_state = RunStatus(run.state)
			if this_runs_state.has_failed():
				batch_error_message = batch_error_message + "\nRun {run_id} failed with status {run_state}".format(run_id=run.run_id, run_state=run.state)
			elif this_runs_state.was_canceled():
				batch_error_message = batch_error_message + "\nRun {run_id} was canceled".format(run_id=run.run_id, run_state=run.state)
			elif batch_error_message == "":
				if current_unanimous_state is None:
					current_unanimous_state = RunStatus(run.state);
				elif current_unanimous_state != this_runs_state:
					current_unanimous_state = RunStatus.UNKNOWN

		if batch_error_message != "":
			raise WorkbenchBatchException("Batch failed because one or more runs failed or was canceled: "+batch_error_message, [batch_id])

		return current_unanimous_state

	# polls a batch job until all jobs reach a desired state
	async def poll_batch_status_until(self, batch_id: str, desired_state : RunStatus, poll_interval:Optional[int]=2, on_state_change : Optional[Callable[[RunStatus, str], None]]=None):

		current_unanimous_state = None
		last_unanimous_state = RunStatus.UNKNOWN
		while current_unanimous_state != desired_state:
			current_unanimous_state = self._get_unanimous_state(batch_id)
			if current_unanimous_state != last_unanimous_state:
				on_state_change(current_unanimous_state, batch_id)
			last_unanimous_state = current_unanimous_state
			await asyncio.sleep(poll_interval)

	async def poll_run_status_until(self, run_id: str, desired_state : RunStatus, poll_interval:Optional[int]=2, on_state_change : Optional[Callable[[RunStatus], None]]=None):
		last_state = None
		current_state = RunStatus.UNKNOWN
		run=None
		while current_state != desired_state:
			run = self.describe_run(run_id)
			current_state = RunStatus(run.state)
			if current_state.has_failed():
				raise WorkbenchRunException("Run "+run.run_id+" has failed with status "+run.state, [run.run_id])
			elif current_state.was_canceled():
				raise WorkbenchCancellationException("Run "+run.run_id+" was canceled with status "+run.state, [run.run_id])
			elif current_state != last_state and on_state_change is not None:
				on_state_change(current_state)
			last_state=current_state
			await asyncio.sleep(poll_interval)

	# waits for the given batch job to reach a particular status.
	async def gather_batch(self, batch_id: str, desired_state : RunStatus, poll_interval:Optional[int]=2, on_state_change : Optional[Callable[[RunStatus, str], None]]=None):
		try:
			await self.poll_batch_status_until(batch_id, desired_state, poll_interval, on_state_change)
		except Exception as ex:
			try:
				self.cancel_batch(batch_id)
			except Exception as ex:
				raise WorkbenchCancellationException(f'Unable to cancel batch {batch_id}') from ex
			else:
				raise ex

	async def gather_runs(self, run_ids: List[str], desired_state : RunStatus, poll_interval:Optional[int]=2, on_state_change : Optional[Callable[[RunStatus, str], None]]=None):
		tasks = []
		for run_id in run_ids:
			task = asyncio.create_task(self.poll_run_status_until(run_id, desired_state, poll_interval, on_state_change), name=run_id)
			tasks.append(task)
		try:
			await asyncio.gather(*tasks)
		except Exception as ex:
			result = self._get_ewes_client().cancel_runs(run_ids)
			if result.run_ids_with_failure is not None and len(result.run_ids_with_failure):
				raise WorkbenchCancellationException(f'Unable to cancel runs after failure/cancellation of run {result.run_ids_with_failure}') from ex
			else:
				raise ex


	### Batch additions
	def _get_workflow_url(self, workflow_id: str, version_id: str) -> str:
		return "{internal_id}/{version_id}".format(internal_id=workflow_id, version_id=version_id)

	def scatter(self, workflow: Workflow, version: WorkflowVersion, engines: Union[List[str], dict]=None, run_constants=None, run_variables=None, tags={}, batch_id=None) -> str:
		# Input checks
		if engines is None:
			# TODO: list engines once we have library support.  Until then, raise exception.
			raise Exception("An engine id must be specified")

		if isinstance(engines, list):
			engine_ids = engines
			default_engine_params={}
		elif isinstance(engines, dict):
			engine_ids = engines.keys()
			default_engine_params=engines
		else:
			raise Exception("Engine information must be given as a list of engine ids, or a dict of engine-ids -> engine configuration")

		if run_constants is not None and type(run_constants) is List:
			if(len(run_constants) !=len(engine_ids)):
				raise Exception("Expected "+len(engine_ids)+" dicts of run constants, but only received "+len(run_constants))

		# TODO: run_variables checking
		tags['federated_analysis'] = datetime.datetime.timestamp(datetime.datetime.now())
		if batch_id is None:
			batch_id = self._get_short_uuid()
		for engine_index in range(0, len(engines)):
			engine_id = engines[engine_index]
			runs_in_batch = run_variables[engine_index]  # the runs in this batch.  an array of dicts.
			batch_run_request = BatchRunRequest(
				workflow_url=self._get_workflow_url(workflow.internalId, version.id),  #BAD URL!
				workflow_type=version.descriptorType,
				#Always omit workflow_type_version, @patrick suggests this field is either unnecessary or read only
				engine_id=engine_id,
				default_workflow_params=run_constants[engine_index] if run_constants is not None and type(run_constants) is list else run_constants,
				default_workflow_engine_parameters = default_engine_params[engine_id] if engine_id in default_engine_params else None,
				default_tags = tags,
				run_requests=[ExtendedRunRequest(**{"workflow_params": run_in_batch}) for run_in_batch in runs_in_batch]
			)
			self.submit_batch(batch_run_request, batch_id=batch_id)
		return batch_id

### end batch additions

	# Being able to stream the logs from a named task seems important, but is missing from the API.
	def stream_task_log_by_task(self, run: Union[ExtendedRun, str], task_name: str, log_type: LogType, max_bytes: Optional[int] = None) -> Iterable[bytes]:
		if type(run) is ExtendedRun:
			run = run.run_id
		for task_log in run.task_logs:
			if task_log.name == task_name:
				return self.stream_task_log(run_id=run, task_id=task_log.task_id, log_type=log_type, max_bytes=max_bytes)
				return self._get_ewes_client().stream_task_logs(run_id=run, task_id=task_log.task_id, log_type=logtype, max_bytes=max_bytes)

	def stream_task_log(self, run: Union[ExtendedRun, str], task_id: str, log_type: LogType, max_bytes: Optional[int] = None) -> Iterable[bytes]:
		return self._get_ewes_client().stream_task_logs(run_id=run, task_id=task_id, log_type=log_type, max_bytes=max_bytes)

	def stream_run_log(self, run: Union[ExtendedRun, str], log_type: LogType, max_bytes: Optional[int] = None) -> Iterable[bytes]:
		if type(run) is ExtendedRun:
			run = run.run_id
		return self._get_ewes_client().stream_run_logs(run_id=run, log_type=log_type, max_bytes=max_bytes)

	# Convenience method: Look up all task logs that have failed.
	def get_failed_task_logs(self, run: Union[ExtendedRun, str]) -> List[Log]:
		loglist=[]
		if type(run) is str:
			run = self.describe_run(run, True)
		for task_log in run.task_logs:
			runstatus = RunStatus(task_log.state)
			if runstatus.has_failed():
				loglist.append(task_log)
		return loglist

def _get_workflow_version(self, version_name:Optional[str]=None) -> WorkflowVersion:
	if version_name is None:
		max_timestamp = 0
		max_version = None
		for version in self.versions:
			timestamp = time.time()
			if (timestamp > max_timestamp):
				max_timestamp = timestamp
				max_version = version
		return max_version
	else:
		for version in self.versions:
			if version.versionName == version_name:
				return version
		raise Exception("Invalid workflow version "+version_name+" specified for workflow "+self.name)

def _get_latest_version(self) -> WorkflowVersion:
	return self.get_workflow_version()


# Monkey patch the workflow object
Workflow.get_latest_version = _get_latest_version
Workflow.get_workflow_version = _get_workflow_version
