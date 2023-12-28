from typing import List, Iterator, Optional, Union, Iterable
from urllib.parse import urljoin

from dnastack.client.models import ServiceEndpoint
from dnastack.client.result_iterator import ResultIterator
from dnastack.client.service_registry.models import ServiceType
from dnastack.client.workbench.base_client import BaseWorkbenchClient, WorkbenchResultLoader
from dnastack.client.workbench.ewes.models import WesServiceInfo, ExtendedRunStatus, ExtendedRunListOptions, \
    ExtendedRunListResponse, ExtendedRun, RunId, WorkbenchApiError, BatchActionResult, MinimalExtendedRun, \
    BatchRunResponse, BatchRunRequest, ExtendedRunRequest, Log, TaskListOptions, \
    TaskListResponse, LogType, ExecutionEngineListOptions, ExecutionEngineListResponse, ExecutionEngine
from dnastack.common.tracing import Span
from dnastack.http.session import HttpSession


class ExtendedRunListResultLoader(WorkbenchResultLoader):

    def __init__(self,
                 service_url: str,
                 http_session: HttpSession,
                 trace: Span,
                 list_options: Optional[ExtendedRunListOptions] = None,
                 max_results: int = None):
        super().__init__(service_url=service_url,
                         http_session=http_session,
                         list_options=list_options,
                         max_results=max_results,
                         trace=trace)

    def get_new_list_options(self) -> ExtendedRunListOptions:
        return ExtendedRunListOptions()

    def extract_api_response(self, response_body: dict) -> ExtendedRunListResponse:
        return ExtendedRunListResponse(**response_body)


class TaskListResultLoader(WorkbenchResultLoader):

    def __init__(self,
                 service_url: str,
                 http_session: HttpSession,
                 trace: Span,
                 list_options: Optional[TaskListOptions] = None,
                 max_results: int = None):
        super().__init__(service_url=service_url,
                         http_session=http_session,
                         list_options=list_options,
                         max_results=max_results,
                         trace=trace)

    def get_new_list_options(self) -> TaskListOptions:
        return TaskListOptions()

    def extract_api_response(self, response_body: dict) -> TaskListResponse:
        return TaskListResponse(**response_body)


class EngineListResultLoader(WorkbenchResultLoader):
    def __init(self,
               service_url: str,
               http_session: HttpSession,
               trace: Span,
               list_options: Optional[ExecutionEngineListOptions] = None,
               max_results: int = None):
        super().__init__(service_url=service_url,
                         http_session=http_session,
                         list_options=list_options,
                         max_results=max_results,
                         trace=trace)

    def get_new_list_options(self) -> ExecutionEngineListOptions:
        return ExecutionEngineListOptions()

    def extract_api_response(self, response_body: dict) -> ExecutionEngineListResponse:
        return ExecutionEngineListResponse(**response_body)


class EWesClient(BaseWorkbenchClient):

    @staticmethod
    def get_adapter_type() -> str:
        return 'ewes-service'

    @staticmethod
    def get_supported_service_types() -> List[ServiceType]:
        return [
            ServiceType(group='com.dnastack.workbench', artifact='ewes-service', version='1.0.0'),
        ]

    # noinspection PyMethodOverriding
    @classmethod
    def make(cls, endpoint: ServiceEndpoint, namespace: str):
        """Create this class with the given `endpoint` and `namespace`."""
        if not endpoint.type:
            endpoint.type = cls.get_default_service_type()
        return cls(endpoint, namespace)

    def get_service_info(self, trace: Optional[Span] = None) -> WesServiceInfo:
        trace = trace or Span(origin=self)
        with self.create_http_session() as session:
            response = session.get(urljoin(self.endpoint.url, f'{self.namespace}/ga4gh/wes/v1/service-info'),
                                   trace_context=trace)
            return WesServiceInfo(**response.json())

    def list_runs(self,
                  list_options: Optional[ExtendedRunListOptions] = None,
                  max_results: int = None,
                  trace: Optional[Span] = None) -> Iterator[ExtendedRunStatus]:
        trace = trace or Span(origin=self)
        return ResultIterator(ExtendedRunListResultLoader(
            service_url=urljoin(self.endpoint.url, f'{self.namespace}/ga4gh/wes/v1/runs'),
            http_session=self.create_http_session(),
            list_options=list_options,
            max_results=max_results,
            trace=trace,
        ))

    def get_status(self, run_id: str, trace: Optional[Span] = None) -> MinimalExtendedRun:
        trace = trace or Span(origin=self)
        with self.create_http_session() as session:
            response = session.get(urljoin(self.endpoint.url, f'{self.namespace}/ga4gh/wes/v1/runs/{run_id}/status'),
                                   trace_context=trace)
            return MinimalExtendedRun(**response.json())

    def get_run(self, run_id: str, include_tasks: bool = False, trace: Optional[Span] = None) -> ExtendedRun:
        trace = trace or Span(origin=self)
        with self.create_http_session() as session:
            response = session.get(urljoin(self.endpoint.url, f'{self.namespace}/ga4gh/wes/v1/runs/{run_id}'
                                                              f'?exclude_tasks={not include_tasks}'),
                                   trace_context=trace)
            return ExtendedRun(**response.json())

    def cancel_run(self, run_id: str, trace: Optional[Span] = None) -> Union[RunId, WorkbenchApiError]:
        with self.create_http_session() as session:
            response = session.post(urljoin(self.endpoint.url, f'{self.namespace}/ga4gh/wes/v1/runs/{run_id}/cancel'),
                                    trace_context=trace)
            return RunId(**response.json())

    def cancel_runs(self, run_ids: List[str], trace: Optional[Span] = None) -> BatchActionResult:
        trace = trace or Span(origin=self)
        with self.create_http_session() as session:
            response = session.post(urljoin(self.endpoint.url, f'{self.namespace}/ga4gh/wes/v1/runs/cancel'),
                                    json=run_ids,
                                    trace_context=trace)
            return BatchActionResult(**response.json())

    def delete_runs(self, run_ids: List[str], trace: Optional[Span] = None) -> BatchActionResult:
        trace = trace or Span(origin=self)
        with self.create_http_session() as session:
            response = session.post(urljoin(self.endpoint.url, f'{self.namespace}/ga4gh/wes/v1/runs/delete'),
                                    json=run_ids,
                                    trace_context=trace)
            return BatchActionResult(**response.json())

    def submit_run(self, data: ExtendedRunRequest, trace: Optional[Span] = None) -> MinimalExtendedRun:
        trace = trace or Span(origin=self)
        with self.create_http_session() as session:
            response = session.post(urljoin(self.endpoint.url, f'{self.namespace}/ga4gh/wes/v1/runs'),
                                    json=data.dict(),
                                    trace_context=trace)
        return MinimalExtendedRun(**response.json())

    def submit_batch(self, data: BatchRunRequest, trace: Optional[Span] = None) -> BatchRunResponse:
        with self.create_http_session() as session:
            response = session.post(urljoin(self.endpoint.url, f'{self.namespace}/ga4gh/wes/v1/batch'),
                                    json=data.dict(),
                                    trace_context=trace)
        return BatchRunResponse(**response.json())

    def stream_run_logs(self,
                        run_id: str,
                        log_type: LogType = LogType.STDOUT,
                        max_bytes: Optional[int] = None,
                        offset: Optional[int] = None,
                        trace: Optional[Span] = None) -> Iterable[bytes]:
        return self.stream_log_url(
            urljoin(self.endpoint.url, f'{self.namespace}/ga4gh/wes/v1/runs/{run_id}/logs/{log_type}'),
            max_bytes,
            offset,
            trace=trace,
        )

    def stream_task_logs(self,
                         run_id: str,
                         task_id: str,
                         log_type: LogType = LogType.STDOUT,
                         max_bytes: Optional[int] = None,
                         offset: Optional[int] = None,
                         trace: Optional[Span] = None) -> Iterable[bytes]:
        return self.stream_log_url(
            urljoin(self.endpoint.url, f'{self.namespace}/ga4gh/wes/v1/runs/{run_id}/tasks/{task_id}/logs/{log_type}'),
            max_bytes,
            offset,
            trace=trace,
        )

    def stream_log_url(self,
                       log_url: str,
                       max_bytes: Optional[int],
                       offset: Optional[int],
                       trace: Optional[Span] = None) -> Iterable[bytes]:
        trace = trace or Span()

        params = dict()
        if max_bytes:
            params['max'] = max_bytes

        if offset:
            params['offset'] = offset

        with self.create_http_session() as session:
            with session.get(urljoin(self.endpoint.url, log_url),
                             params=params,
                             stream=True,
                             trace_context=trace) as response:
                if int(response.headers['Content-Length']) == 0:
                    yield None
                    return
                for chunk in response.iter_content(chunk_size=None):
                    yield chunk

    def list_tasks(self,
                   run_id: str,
                   list_options: Optional[TaskListOptions] = None,
                   max_results: int = None,
                   trace: Optional[Span] = None) -> Iterator[Log]:
        trace = trace or Span(origin=self)
        return ResultIterator(TaskListResultLoader(
            service_url=urljoin(self.endpoint.url, f'{self.namespace}/ga4gh/wes/v1/runs/{run_id}/tasks'),
            http_session=self.create_http_session(),
            list_options=list_options,
            max_results=max_results,
            trace=trace,
        ))

    def list_engines(self,
                     list_options: ExecutionEngineListOptions,
                     max_results: int = None,
                     trace: Optional[Span] = None) -> Iterable[ExecutionEngine]:
        trace = trace or Span(origin=self)
        return ResultIterator(EngineListResultLoader(
            service_url=urljoin(self.endpoint.url, f'{self.namespace}/engines'),
            http_session=self.create_http_session(),
            list_options=list_options,
            max_results=max_results,
            trace=trace,
        ))

    def get_engine(self,
                   engine_id: str,
                   trace: Optional[Span] = None) -> ExecutionEngine:
        with self.create_http_session() as session:
            response = session.get(urljoin(self.endpoint.url, f'{self.namespace}/engines/{engine_id}'),
                                   trace_context=trace)
            return ExecutionEngine(**response.json())
