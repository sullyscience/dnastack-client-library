from abc import ABC
from typing import Optional, List
from uuid import uuid4

from requests.auth import AuthBase

from dnastack.client.models import ServiceEndpoint
from dnastack.client.service_registry.models import ServiceType
from dnastack.common.events import EventSource
from dnastack.common.logger import get_logger
from dnastack.feature_flags import in_global_debug_mode
from dnastack.http.authenticators.factory import HttpAuthenticatorFactory
from dnastack.http.session import HttpSession


class BaseServiceClient(ABC):
    """ The base class for all DNAStack Clients """

    def __init__(self, endpoint: ServiceEndpoint):
        if not endpoint.url.endswith(r'/'):
            endpoint.url = endpoint.url + r'/'

        self._uuid = str(uuid4())
        self._endpoint = endpoint
        self._logger = get_logger(f'{type(self).__name__}/{self._endpoint.id}'
                                  if in_global_debug_mode
                                  else type(self).__name__)
        self._current_authenticator: Optional[AuthBase] = None
        self._events = EventSource(['authentication-before',
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

    @property
    def events(self) -> EventSource:
        return self._events

    @property
    def endpoint(self):
        return self._endpoint

    def __del__(self):
        self.close()

    def close(self):
        if hasattr(self, '_events'):
            self._events.clear()

    @staticmethod
    def get_adapter_type() -> str:
        """Get the descriptive adapter type"""
        raise NotImplementedError()

    @staticmethod
    def get_supported_service_types() -> List[ServiceType]:
        """ The list of supported service types

            The first one is always regarded as the default type.
        """
        raise NotImplementedError()

    @classmethod
    def get_default_service_type(cls) -> ServiceType:
        return cls.get_supported_service_types()[0]

    @property
    def url(self):
        """The base URL to the endpoint"""
        return self._endpoint.url

    def require_authentication(self) -> bool:
        return len(self._endpoint.get_authentications()) > 0

    def create_http_session(self,
                            suppress_error: bool = False,
                            no_auth: bool = False) -> HttpSession:
        """Create HTTP session wrapper"""
        session = HttpSession(self._endpoint.id,
                              HttpAuthenticatorFactory.create_multiple_from(endpoint=self._endpoint),
                              suppress_error=suppress_error,
                              enable_auth=(not no_auth))
        self.events.set_passthrough(session.events)
        return session

    @classmethod
    def make(cls, endpoint: ServiceEndpoint):
        """Create this class with the given `endpoint`."""
        if not endpoint.type:
            endpoint.type = cls.get_default_service_type()

        return cls(endpoint)
