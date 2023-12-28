import re

from abc import ABC
from typing import Any, Dict, List

from dnastack.common.events import EventSource
from dnastack.common.tracing import Span
from dnastack.http.authenticators.oauth2_adapter.models import OAuth2Authentication
from dnastack.common.logger import get_logger


class AuthException(RuntimeError):
    def __init__(self, msg: str = None, url: str = None):
        super().__init__()

        self.msg = msg
        self.url = url

    def __repr__(self):
        if self.url:
            return f"{self.url}: {self.msg or ''}"
        else:
            return self.msg or ''

    def __str__(self):
        return self.__repr__()


class OAuth2Adapter(ABC):
    def __init__(self, auth_info: OAuth2Authentication):
        self._auth_info = auth_info
        self._events = EventSource(['blocking-response-required', 'blocking-response-ok', 'blocking-response-failed'],
                                   origin=self)
        self._logger = get_logger(f'{type(self).__name__}/{self._auth_info.get_content_hash()[:8]}')

    @property
    def events(self) -> EventSource:
        return self._events

    @property
    def auth_info(self) -> OAuth2Authentication:
        return self._auth_info

    def check_config_readiness(self):
        property_names = self.get_expected_auth_info_fields()
        auth = self.auth_info

        missing_property_names = [
            property_name
            for property_name in property_names
            if not hasattr(auth, property_name) or not getattr(auth, property_name)
        ]

        if missing_property_names:
            raise AssertionError(f"{type(self).__module__}.{type(self).__name__}: {self._auth_info}: Missing {', '.join(missing_property_names)}")

    @classmethod
    def is_compatible_with(cls, auth_info: OAuth2Authentication) -> bool:
        property_names = cls.get_expected_auth_info_fields()
        auth = auth_info

        for property_name in property_names:
            if not hasattr(auth, property_name) or not getattr(auth, property_name):
                return False

        return True

    @staticmethod
    def get_expected_auth_info_fields() -> List[str]:
        raise NotImplementedError()

    def exchange_tokens(self, trace_context: Span) -> Dict[str, Any]:
        """
        :raises AuthException: raised when the authentication fails
        """
        raise NotImplementedError()

    def _prepare_resource_urls_for_request(self, original_resource_urls: str) -> str:
        return ','.join(re.split(r'\s+', original_resource_urls))
