import json
from abc import ABC
from json import JSONDecodeError
from typing import Any, Dict, Union, Optional

from pydantic import BaseModel
from requests import Request, Session
from requests.auth import AuthBase

from dnastack.client.models import ServiceEndpoint
from dnastack.common.events import EventSource
from dnastack.common.logger import get_logger
from dnastack.common.tracing import Span
from dnastack.http.session_info import SessionInfo


class AuthenticationRequired(RuntimeError):
    """ Raised when the client needs to initiate the authentication process for the first time """


class ReauthenticationRequired(RuntimeError):
    """ Raised when the authenticator needs to initiate the re-authentication process """

    def __init__(self, message: str):
        super().__init__(message)


class ReauthenticationRequiredDueToConfigChange(ReauthenticationRequired):
    """ Raised when the authenticator needs to initiate the re-authentication process due to config change"""


class RetryWithFallbackAuthentication(RuntimeError):
    """ Raised when the authenticator needs to use a fallback authorization before retrying """


class RefreshRequired(RuntimeError):
    """ Raised when the authenticator needs to initiate the refresh process """

    def __init__(self, session: Optional[SessionInfo]):
        super().__init__('Session refresh required')
        self.__session = session.copy(deep=True)

    @property
    def session(self):
        return self.__session


class NoRefreshToken(RuntimeError):
    """ Raised when the authenticator attempts to refresh tokens but the refresh token is not defined """

    def __init__(self):
        super().__init__('No refresh token')


class FeatureNotAvailable(RuntimeError):
    """ Raised when the authenticator does not support a particular feature. This can be safely ignored. """

    def __init__(self):
        super().__init__('Feature not available')


class InvalidStateError(RuntimeError):
    def __init__(self, message: str, details: Dict[str, Any]):
        super().__init__()
        self.__message = message
        self.__details = details

    def __str__(self):
        error_msg = self.__message

        try:
            error_json = json.dumps({
                k: v
                for k, v in self.__details.items()
                if v is not None
            }, sort_keys=True)
            error_msg = f"{error_msg} ({error_json})"
        except JSONDecodeError:
            pass

        return error_msg


class AuthStateStatus:
    READY = 'ready'
    UNINITIALIZED = 'uninitialized'
    REFRESH_REQUIRED = 'refresh-required'
    REAUTH_REQUIRED = 'reauth-required'


class AuthState(BaseModel):
    authenticator: str
    id: str
    auth_info: Dict[str, Any]
    session_info: Dict[str, Any]
    status: str  # See AuthStateStatus


class Authenticator(AuthBase, ABC):
    def __init__(self):
        self._events = EventSource(['authentication-before',
                                    'authentication-ok',
                                    'authentication-failure',
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
        self._logger = get_logger(f'{type(self).__name__}')

    @property
    def events(self) -> EventSource:
        return self._events

    @property
    def fully_qualified_class_name(self):
        t = type(self)
        return f'{t.__module__}.{t.__name__}'

    @property
    def class_name(self):
        return type(self).__name__

    @property
    def session_id(self):
        raise NotImplementedError()

    def initialize(self, trace_context: Span) -> SessionInfo:
        """ Initialize the authenticator """
        self.events.dispatch('initialization-before', dict(origin=f'{self.class_name}'))
        logger = trace_context.create_span_logger(self._logger)
        try:
            logger.debug('initialize: Restoring...')
            info = self.restore_session()
            self.events.dispatch('session-restored', None)
            logger.debug('initialize: Restored')
            return info
        except (AuthenticationRequired, ReauthenticationRequired) as _:
            logger.debug('initialize: Initiating the authentication...')
            return self.authenticate(trace_context)
        except RefreshRequired as refresh_exception:
            logger.debug('initialize: Initiating the token refresh...')
            try:
                return self.refresh(trace_context)
            except ReauthenticationRequired as _:
                logger.debug('initialize: Failed to refresh tokens. Initiating the re-authentication...')
            return self.authenticate(trace_context)

    def authenticate(self, trace_context: Span) -> SessionInfo:
        """ Force-initiate the authorization process """
        raise NotImplementedError()

    def refresh(self, trace_context: Optional[Span] = None) -> SessionInfo:
        """
        Refresh the session

        :raises NoRefreshToken: This indicates that the refresh token is undefined.
        :raises ReauthorizationRequired: The stored session exists but it does not contain enough information to initiate the refresh process.
        :raises FeatureNotAvailable: The feature is not available and the caller may ignore this exception.
        :raises InvalidStateError: When the refresh
        """
        raise NotImplementedError()

    def revoke(self):
        """
        Revoke the session and remove the corresponding session info

        :raises FeatureNotAvailable: The feature is not available and the caller may ignore this exception.
        """
        raise NotImplementedError()

    def restore_session(self) -> SessionInfo:
        """
        Only restore the session info

        :raises AuthenticationRequired: When the authentication is required
        :raises ReauthenticationRequired: When the re-authentication is required
        :raises RefreshRequired: When the token refresh is required
        """
        raise NotImplementedError()

    def before_request(self, r: Union[Request, Session], trace_context: Span):
        logger = trace_context.create_span_logger(self._logger)
        logger.debug('before_request: BEGIN')
        self.update_request(self.initialize(trace_context), r)
        logger.debug('before_request: END')

    def update_request(self, session: SessionInfo, r: Union[Request, Session]) -> Union[Request, Session]:
        """ Update the session/request object with session info """
        raise NotImplementedError()

    def get_state(self) -> AuthState:
        """ Retrieve the current state of the authenticator """
        raise NotImplementedError()

    def __call__(self, r: Request):
        # FIXME This may not be used as we manually invoke the authenticator due to
        #  our complicate authentication procedure. We may need to remove this.
        self._logger.warning("The authenticator is called directly and is not connected to any trace context.")
        span = Span(origin=self)
        self.before_request(r, trace_context=span)
        return r

    @classmethod
    def make(cls, endpoint: ServiceEndpoint, auth_info: Dict[str, Any]):
        raise NotImplementedError()
