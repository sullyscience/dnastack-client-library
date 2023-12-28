import platform
import sys
from contextlib import AbstractContextManager
from typing import List, Optional, Any
from uuid import uuid4

from pydantic import BaseModel
from requests import Session, Response

from dnastack.common.events import EventSource
from dnastack.common.logger import get_logger
from dnastack.common.tracing import Span
from dnastack.constants import __version__
from dnastack.http.authenticators.abstract import Authenticator


class AuthenticationError(RuntimeError):
    """ Authentication Error """


class HttpError(RuntimeError):
    def __init__(self, response: Response, trace_context: Optional[Span] = None):
        super(HttpError, self).__init__(response, trace_context)

    @property
    def response(self) -> Response:
        return self.args[0]

    @property
    def trace(self) -> Span:
        return self.args[1]

    def __str__(self):
        response: Response = self.response

        error_feedback = f'HTTP {response.status_code}'

        # Handle the response body.
        response_text = response.text.strip()
        if len(response_text) == 0:
            error_feedback = f'{error_feedback} (empty response)'
        else:
            error_feedback = f'{error_feedback}: {response_text}'

        return error_feedback


class ClientError(HttpError):
    pass


class ServerError(HttpError):
    pass


class JsonPatch(BaseModel):
    op: str
    path: Optional[str] = None
    value: Optional[Any] = None


class RetryHistoryEntry(BaseModel):
    authenticator_index: int
    with_reauthentication: bool
    with_next_authenticator: bool
    encountered_http_status: int
    encountered_http_response: str
    resolution: str

    def __str__(self):
        return (f'>>> Auth #{self.authenticator_index}\n'
                f'>>> reauth: {self.with_reauthentication}\n'
                f'>>> use_next: {self.with_next_authenticator}\n\n'
                f'HTTP {self.encountered_http_status}\n\n{self.encountered_http_response}\n\n'
                f'[ → {self.resolution}]')


class HttpSession(AbstractContextManager):
    def __init__(self,
                 uuid: Optional[str] = None,
                 authenticators: List[Authenticator] = None,
                 suppress_error: bool = True,
                 enable_auth: bool = True,
                 session: Optional[Session] = None):
        super().__init__()

        self.__id = uuid or str(uuid4())
        self.__logger = get_logger(f'{type(self).__name__}/{self.__id}')
        self.__authenticators = authenticators
        self.__session: Optional[Session] = session
        self.__suppress_error = suppress_error
        self.__enable_auth = enable_auth

        # This will inherit event types from
        self.__events = EventSource(['authentication-before',
                                     'authentication-ok',
                                     'authentication-failure',
                                     'authentication-ignored',
                                     'blocking-response-required',
                                     'blocking-response-ok',
                                     'blocking-response-failed',
                                     'initialization-before',
                                     'refresh-before',
                                     'refresh-ok',
                                     'refresh-failure',
                                     'session-restored',
                                     'session-not-restored',
                                     'session-revoked'],
                                    origin=self)

        if self.__authenticators:
            for authenticator in self.__authenticators:
                self.__events.set_passthrough(authenticator.events)

        if not self.__enable_auth:
            self.__logger.info('Authentication has been disable for this session.')

    @property
    def events(self) -> EventSource:
        return self.__events

    @property
    def _session(self) -> Session:
        if not self.__session:
            self.__session = Session()
            self.__session.headers.update({
                'User-Agent': self.generate_http_user_agent()
            })

        return self.__session

    def __enter__(self):
        super().__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        self.close()

    def submit(self,
               method: str,
               url: str,
               retry_with_reauthentication: bool = True,
               retry_with_next_authenticator: bool = False,
               authenticator_index: int = 0,
               retry_history: Optional[List[RetryHistoryEntry]] = None,
               trace_context: Optional[Span] = None,
               **kwargs) -> Response:
        trace_context = trace_context or Span(origin=self)

        retry_history = retry_history or list()
        session = self._session

        logger = trace_context.create_span_logger(self.__logger)
        params = kwargs.get('params', None)
        logger.debug(f'{method.upper()} {url} {params} (AUTH: {"Enabled" if self.__enable_auth else "Disabled"})')

        authenticator: Optional[Authenticator] = None

        if self.__enable_auth:
            if self.__authenticators:
                if authenticator_index < len(self.__authenticators):
                    authenticator = self.__authenticators[authenticator_index]
                else:
                    logger.error(f'Failed to authenticate for {url}')
                    counter = 0
                    for retry in retry_history:
                        counter += 1
                        logger.error(f'Retry #{counter}:\n\n{retry}\n')

                    raise AuthenticationError('Exhausted all authentication methods but still unable to get successful '
                                              f'authentication for {url}')

                logger.debug(f'AUTH: session_id => {authenticator.session_id}')

                authenticator.before_request(session, trace_context=trace_context)
            else:
                logger.debug(f'AUTH: no authenticators configured')
        else:
            logger.debug(f'AUTH: the authentication has been disabled')
            self.events.dispatch('authentication-ignored', dict(method=method, url=url))

        http_method = method.lower()

        trace_metadata = {
            'auth_enabled': self.__enable_auth,
            'request': {
                'method': http_method,
                'url': url,
            }
        }

        with trace_context.new_span(metadata=trace_metadata) as sub_span:
            sub_logger = sub_span.create_span_logger(logger)
            existing_headers = kwargs.get('headers') or dict()
            existing_headers.update(sub_span.create_http_headers())
            kwargs['headers'] = existing_headers

            response = getattr(self._session, http_method)(url, **kwargs)

            sub_logger.debug(f'Response/URL {url}')
            sub_logger.debug(f'Response/HTTP {response.status_code} ({len(response.text)}B)')
            sub_logger.debug(f'Response/Body:\n{response.text}')

        if response.ok:
            return response
        else:
            if self.__suppress_error:
                logger.debug('Error suppressed by the caller of this method.')
                return response

            status_code = response.status_code

            if self.__enable_auth:
                if status_code == 401 and authenticator:
                    authenticator.revoke()

                    retry = RetryHistoryEntry(url=url,
                                              authenticator_index=authenticator_index,
                                              with_reauthentication=retry_with_reauthentication,
                                              with_next_authenticator=retry_with_next_authenticator,
                                              encountered_http_status=status_code,
                                              encountered_http_response=response.text,
                                              resolution='')
                    retry_history.append(retry)

                    if retry_with_reauthentication:
                        # Initiate the reauthorization process.
                        retry.resolution = 'retry with re-authentication'

                        return self.submit(method,
                                           url,
                                           retry_with_reauthentication=False,
                                           retry_with_next_authenticator=True,
                                           authenticator_index=authenticator_index,
                                           retry_history=retry_history,
                                           trace_context=trace_context,
                                           **kwargs)
                    elif retry_with_next_authenticator:
                        retry.resolution = 'retry with the next authenticator'

                        return self.submit(method,
                                           url,
                                           retry_with_reauthentication=True,
                                           retry_with_next_authenticator=False,
                                           authenticator_index=authenticator_index + 1,
                                           retry_history=retry_history,
                                           trace_context=trace_context,
                                           **kwargs)
                    else:
                        raise RuntimeError('Invalid state')
                else:
                    # Non-access-denied error will be handled here.
                    self._raise_http_error(response, trace_context=trace_context)
            else:
                # No-auth requests will just throw an exception.
                self._raise_http_error(response, trace_context=trace_context)
        # End if response is not OK.

    def get(self, url, trace_context: Optional[Span] = None, **kwargs) -> Response:
        return self.submit(method='get',
                           url=url,
                           trace_context=trace_context,
                           **kwargs)

    def post(self, url, trace_context: Optional[Span] = None, **kwargs) -> Response:
        return self.submit(method='post',
                           url=url,
                           trace_context=trace_context,
                           **kwargs)

    def delete(self, url, trace_context: Optional[Span] = None, **kwargs) -> Response:
        return self.submit(method='delete',
                           url=url,
                           trace_context=trace_context,
                           **kwargs)

    def json_patch(self, url, trace_context: Optional[Span] = None, **kwargs) -> Response:
        headers = kwargs.setdefault('headers', {})
        headers['Content-Type'] = 'application/json-patch+json'
        return self.submit(method='patch',
                           url=url,
                           trace_context=trace_context,
                           **kwargs)

    def close(self):
        if self.__session:
            self.__id = None
            self.__session.close()
            self.__session = None

    @property
    def authenticators(self):
        return self.__authenticators

    def _raise_http_error(self, response: Response, trace_context: Span):
        raise (ClientError if response.status_code < 500 else ServerError)(response, trace_context=trace_context)

    def __del__(self):
        self.close()

    @staticmethod
    def generate_http_user_agent(comments: Optional[List[str]] = None) -> str:
        # NOTE: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/User-Agent
        interested_module_names = [
            'IPython',  # indicates that it is probably used in a notebook
            'unittest',  # indicates that it is used by a test code
        ]

        final_comments = [
            f'Platform/{platform.platform()}',  # OS information + CPU architecture
            'Python/{}.{}.{}'.format(*sys.version_info),  # Python version
            *(comments or list()),
            *[
                f'Module/{interested_module_name}'
                for interested_module_name in interested_module_names
                if interested_module_name in sys.modules
            ]
        ]

        return f'dnastack-client/{__version__} {" ".join(final_comments)}'.strip()
