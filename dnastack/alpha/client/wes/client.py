# The implementation is based on https://github.com/ga4gh/workflow-execution-service-schemas/tree/develop/openapi.
import os.path
from mimetypes import guess_type

from pprint import pformat

import json
from datetime import datetime
from pydantic import BaseModel, ValidationError, Field
from typing import Iterator, Optional, List, Any, Dict, Union
from urllib.parse import urljoin

from dnastack.client.base_client import BaseServiceClient
from dnastack.client.base_exceptions import UnauthenticatedApiAccessError, UnauthorizedApiAccessError, DataConnectError
from dnastack.client.result_iterator import ResultLoader, InactiveLoaderError, ResultIterator
from dnastack.client.service_registry.models import ServiceType, Service
from dnastack.http.session import HttpSession, HttpError, ClientError

STANDARD_WES_TYPE_V1_1 = ServiceType(group='org.ga4gh', artifact='wes', version='1.1')
STANDARD_WES_TYPE_V1_0 = ServiceType(group='org.ga4gh', artifact='wes', version='1.0')


class ErrorResponse(BaseModel):
    ...


class _FormDataEntry(BaseModel):
    name: str
    value: str


class RunRequest(BaseModel):
    workflow_url: str
    workflow_params: Optional[Dict[str, Any]] = Field(default_factory=dict)
    workflow_engine_parameters: Optional[Union[str, Dict[str, Any]]] = None
    workflow_type: str = 'WDL'
    workflow_type_version: str = '1.0'
    tags: Optional[Union[str, Dict[str, Any]]] = Field(default_factory=dict)

    # Auxiliary properties
    attachments: Optional[List[str]] = None

    def to_form_data(self):
        multipart_data = [
            ('workflow_url', (None, self.workflow_url)),
            ('workflow_type', (None, self.workflow_type)),
            ('workflow_type_version', (None, self.workflow_type_version)),
        ]

        # Add optional JSON parameters.
        for property_name in ['workflow_params', 'workflow_engine_parameters', 'tags']:
            property_value = getattr(self, property_name)

            if not property_value:
                property_value = '{}'
            elif isinstance(property_value, dict):
                property_value = json.dumps(property_value)

            multipart_data.append((property_name, (None, property_value, 'application/json')))

        # Handle file attachments.
        if self.attachments:
            for i in range(len(self.attachments)):
                param_name = 'workflow_attachment'
                attachment_url = self.attachments[i]
                guessed = guess_type(attachment_url)

                with open(attachment_url, 'rb') as f:
                    multipart_data.append((
                        param_name,
                        (
                            os.path.basename(attachment_url),  # file name
                            f.read(),  # binary
                            guessed[0] if guessed and guessed[0] else None,
                        )
                    ))

        return dict(
            # data=form_data,
            files=multipart_data
        )


class _Id(BaseModel):
    run_id: str


class _Log(BaseModel):
    """
    Log and other info

    https://github.com/ga4gh/workflow-execution-service-schemas/blob/develop/openapi/components/schemas/Log.yaml
    """
    name: Optional[str] = None
    cmd: Optional[Union[str, List[str]]] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    stdout: Optional[str] = None  # URL
    stderr: Optional[str] = None  # URL
    exit_code: Optional[int] = None


class _Run(BaseModel):
    # https://github.com/ga4gh/workflow-execution-service-schemas/blob/develop/openapi/components/schemas/RunLog.yaml
    run_id: str
    request: RunRequest

    # State can take any of the following values:
    #  - UNKNOWN: The state of the task is unknown. This provides a safe default for messages where this field is
    #             missing, for example, so that a missing field does not accidentally imply that the state is QUEUED.
    #  - QUEUED: The task is queued.
    #  - INITIALIZING: The task has been assigned to a worker and is currently preparing to run. For example, the
    #                  worker may be turning on, downloading input files, etc.
    #  - RUNNING: The task is running. Input files are downloaded and the first Executor has been started.
    #  - PAUSED: The task is paused. An implementation may have the ability to pause a task, but this is not required.
    #  - COMPLETE: The task has completed running. Executors have exited without error and output files have been
    #              successfully uploaded.
    #  - EXECUTOR_ERROR: The task encountered an error in one of the Executor processes. Generally, this means that
    #                    an Executor exited with a non-zero exit code.
    #  - SYSTEM_ERROR: The task was stopped due to a system error, but not from an Executor, for example an upload
    #                  failed due to network issues, the worker's ran out of disk space, etc.
    #  - CANCELED: The task was canceled by the user.
    #  - CANCELING: The task was canceled by the user, and is in the process of stopping.
    #
    # https://github.com/ga4gh/workflow-execution-service-schemas/blob/develop/openapi/components/schemas/State.yaml
    state: str

    run_log: _Log
    task_logs: List[_Log]
    outputs: Optional[Dict[str, str]] = Field(default_factory=dict)


class _Status(BaseModel):
    """
    Small description of a workflow run, returned by server during listing

    https://github.com/ga4gh/workflow-execution-service-schemas/blob/develop/openapi/components/schemas/RunStatus.yaml
    """
    run_id: str
    state: str  # See _Run.state


class RunListResponse(BaseModel):
    runs: List[_Status]
    next_page_token: Optional[str] = None


class RunListLoader(ResultLoader):
    def __init__(self,
                 initial_url: str,
                 page_size: Optional[int] = None,
                 page_token: Optional[str] = None,
                 http_session: Optional[HttpSession] = None):
        self.__http_session = http_session
        self.__initial_url = initial_url
        self.__page_size = page_size
        self.__page_token = page_token
        self.__current_url: Optional[str] = None
        self.__active = True
        self.__visited_urls: List[str] = Field(default_factory=list)

    def load(self) -> List[_Status]:
        if not self.__active:
            raise InactiveLoaderError(self.__initial_url)

        with self.__http_session as session:
            current_url = self.__initial_url

            params = dict()
            if self.__page_size:
                params['page_size'] = self.__page_size
            if self.__page_token:
                params['page_token'] = self.__page_token

            try:
                response = session.get(current_url, params=params)
            except HttpError as e:
                status_code = e.response.status_code
                response_text = e.response.text

                self.__visited_urls.append(current_url)

                if status_code == 401:
                    raise UnauthenticatedApiAccessError(self.__generate_api_error_feedback(response_text))
                elif status_code == 403:
                    raise UnauthorizedApiAccessError(self.__generate_api_error_feedback(response_text))
                elif status_code >= 400:  # Catch all errors
                    raise DataConnectError(
                        f'Unexpected error: {response_text}',
                        status_code,
                        response_text,
                        urls=self.__visited_urls
                    )

            status_code = response.status_code
            response_text = response.text

            try:
                response_body = response.json() if response_text else dict()
            except Exception:
                self.logger.error(f'{self.__initial_url}: Unexpectedly non-JSON response body from {current_url}')
                raise DataConnectError(
                    f'Unable to deserialize JSON from {response_text}.',
                    status_code,
                    response_text,
                    urls=self.__visited_urls
                )

            try:
                api_response = RunListResponse(**response_body)
            except ValidationError:
                raise DataConnectError(
                    f'Invalid Response Body: {response_body}',
                    status_code,
                    response_text,
                    urls=self.__visited_urls
                )

            self.logger.debug(f'Response:\n{pformat(response_body, indent=2)}')

            self.__page_token = api_response.next_page_token or None
            if not self.__page_token:
                self.__active = False

            return api_response.runs

    def has_more(self) -> bool:
        return self.__active or self.__current_url

    def __generate_api_error_feedback(self, response_body) -> str:
        if self.__current_url:
            return f'Failed to load a follow-up page of the table list from {self.__current_url} ({response_body})'
        else:
            return f'Failed to load the first page of the table list from {self.__initial_url} ({response_body})'


class LogOutput(BaseModel):
    origin: _Log
    stdout: Optional[str] = None
    stderr: Optional[str] = None

    def is_empty(self) -> bool:
        return not self.stdout and not self.stderr


class Run:
    def __init__(self, session: Optional[HttpSession] = None, base_url: Optional[str] = None):
        self.__session = session
        self.__base_url = base_url if base_url.endswith('/') else (base_url + '/')
        self.__id = None

    def connect(self, session: HttpSession):
        assert self.__session is not None, 'This Run object has already been attached to a WES client.'
        self.__session = session

    def info(self) -> _Run:
        # GET /runs/{id}
        raw_data = self.__session.get(self.__base_url).json()
        try:
            return _Run(**raw_data)
        except ValidationError:
            raise RuntimeError(f'Unexpected Response: {raw_data}')

    @property
    def id(self) -> str:
        if not self.__id:
            self.__id = self.info().run_id
        return self.__id

    @property
    def status(self) -> str:
        # GET /runs/{id}/status
        response = self.__session.get(urljoin(self.__base_url, 'status'))
        return _Status(**response.json()).state

    def cancel(self):
        # POST /runs/{id}/cancel
        self.__session.post(urljoin(self.__base_url, 'cancel'))

    def get_logs(self, include_stderr: bool = False) -> Iterator[LogOutput]:
        info = self.info()

        for log_record in info.task_logs + [info.run_log]:
            output = LogOutput(origin=log_record)

            if log_record.stdout:
                try:
                    output.stdout = self.__session.get(log_record.stdout).text.strip()
                except ClientError as e:
                    if e.response.status_code != 404:
                        raise e

            if include_stderr and log_record.stderr:
                try:
                    output.stderr = self.__session.get(log_record.stderr).text.strip()
                except ClientError as e:
                    if e.response.status_code != 404:
                        raise e

            yield output


class WesClient(BaseServiceClient):
    @staticmethod
    def get_adapter_type() -> str:
        return 'wes'

    @staticmethod
    def get_supported_service_types() -> List[ServiceType]:
        return [
            STANDARD_WES_TYPE_V1_0,
            STANDARD_WES_TYPE_V1_1,
        ]

    def get_service_info(self):
        with self.create_http_session() as session:
            response = session.get(urljoin(self.endpoint.url, f'service-info'))
            return response.json()

    def get_runs(self, page_size: Optional[int] = None, page_token: Optional[str] = None) -> Iterator[_Run]:
        # GET /runs
        return ResultIterator(RunListLoader(initial_url=urljoin(self.endpoint.url, f'runs'),
                                            page_size=page_size,
                                            page_token=page_token,
                                            http_session=self.create_http_session()))

    def run(self, id: str) -> Run:
        return Run(self.create_http_session(), urljoin(self.endpoint.url, f'runs/{id}'))

    def submit(self, run: RunRequest) -> str:
        workflow_url_is_external = run.workflow_url.startswith('http://') or run.workflow_url.startswith('https://')
        workflow_url_is_in_attachments = run.workflow_url in [os.path.basename(p) for p in (run.attachments or list())]
        if not workflow_url_is_external and not workflow_url_is_in_attachments:
            if not workflow_url_is_in_attachments:
                raise RuntimeError('The workflow file from the local drive is defined but it is apparently not in the '
                                   'attachments of this run request. Please check your request object. '
                                   f'(Given: {run})')
            else:
                raise RuntimeError(f'The given workflow URL is not supported. (Given: {run.workflow_url})')

        with self.create_http_session() as session:
            response = session.post(urljoin(self.endpoint.url, 'runs'),
                                    **run.to_form_data())
            return _Id(**response.json()).run_id
