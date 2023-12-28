from abc import ABC
from pprint import pformat
from typing import Optional, List

from pydantic import ValidationError
from requests import Response

from dnastack import ServiceEndpoint
from dnastack.alpha.client.workbench.models import BaseListOptions, PaginatedResource
from dnastack.client.base_client import BaseServiceClient
from dnastack.client.base_exceptions import UnauthenticatedApiAccessError, UnauthorizedApiAccessError
from dnastack.client.result_iterator import ResultLoader, InactiveLoaderError
from dnastack.common.tracing import Span
from dnastack.http.authenticators.factory import HttpAuthenticatorFactory
from dnastack.http.authenticators.oauth2 import OAuth2Authenticator
from dnastack.http.session import HttpSession, HttpError, ServerError


class ApiError(Exception):
    def __init__(self, message, status_code, text):
        self.message = message
        self.status_code = status_code
        self.text = text

    def __str__(self):
        return self.message


class PageableApiError(ApiError):
    def __init__(self, message, status_code, text, urls):
        super().__init__(message, status_code, text)
        self.urls = urls


class NamespaceError(RuntimeError):
    """ Raised when the access to the API requires an authentication. """

    def __init__(self, message: str):
        super(NamespaceError, self).__init__(f'Namespace error: {message}')


class BaseWorkbenchClient(BaseServiceClient, ABC):
    def __init__(self, endpoint: ServiceEndpoint, namespace: Optional[str] = None):
        super().__init__(endpoint)
        if namespace:
            self.__namespace = namespace
        else:
            self.__namespace = self.__extract_namespace_from_auth(endpoint)

        self._logger.debug(f"Authenticated workbench services for namespace {self.__namespace}")

    @property
    def namespace(self):
        return self.__namespace

    @classmethod
    def __extract_namespace_from_auth(cls, endpoint: ServiceEndpoint) -> str:
        for authenticator in HttpAuthenticatorFactory.create_multiple_from(endpoint=endpoint):
            if isinstance(authenticator, OAuth2Authenticator):
                session_info = authenticator.initialize(trace_context=Span(origin=cls.__name__))
                return session_info.access_token_claims().sub
        raise NamespaceError("Could not extract namespace from request and no value was provided")


class WorkbenchResultLoader(ResultLoader):
    def __init__(self,
                 service_url: str,
                 http_session: HttpSession,
                 list_options: Optional[BaseListOptions] = None,
                 max_results: int = None):
        self.__http_session = http_session
        self.__service_url = service_url
        self.__list_options = list_options
        self.__max_results = int(max_results) if max_results else None
        self.__loaded_results = 0
        self.__active = True
        self.__visited_urls: List[str] = list()

        if not self.__list_options:
            self.__list_options = self.get_new_list_options()

    def has_more(self) -> bool:
        return self.__active

    def __generate_api_error_feedback(self, response_body) -> str:
        if self.__service_url:
            return f'Failed to load the next page of data from {self.__service_url}: ({response_body})'
        else:
            return f'Failed to load the next page of data: ({response_body})'

    def get_new_list_options(self) -> BaseListOptions:
        return BaseListOptions()

    def extract_api_response(self, response_body: dict) -> PaginatedResource:
        pass

    def load(self) -> List[any]:
        if not self.__active:
            raise InactiveLoaderError(self.__service_url)

        with self.__http_session as session:
            current_url = self.__service_url

            try:
                response = session.get(current_url, params=self.__list_options)
            except HttpError as e:
                status_code = e.response.status_code
                response_text = e.response.text

                self.__visited_urls.append(current_url)

                if status_code == 401:
                    raise UnauthenticatedApiAccessError(self.__generate_api_error_feedback(response_text))
                elif status_code == 403:
                    raise UnauthorizedApiAccessError(self.__generate_api_error_feedback(response_text))
                elif status_code >= 400:  # Catch all errors
                    raise PageableApiError(
                        f'Unexpected error: {response_text}',
                        status_code,
                        response_text,
                        urls=self.__visited_urls
                    )

            status_code = response.status_code
            response_text = response.text

            try:
                response_body = response.json() if response_text else dict()
            except Exception as e:
                self.logger.error(f'{self.__service_url}: Unexpectedly non-JSON response body from {current_url}')
                raise PageableApiError(
                    f'Unable to deserialize JSON from {response_text}.',
                    status_code,
                    response_text,
                    urls=self.__visited_urls
                )

            try:
                api_response = self.extract_api_response(response_body)
            except ValidationError as e:
                raise PageableApiError(
                    f'Invalid Response Body: {response_body}',
                    status_code,
                    response_text,
                    urls=self.__visited_urls
                )

            self.logger.debug(f'Response:\n{pformat(response_body, indent=2)}')

            self.__list_options.page_token = api_response.next_page_token or None
            if not self.__list_options.page_token:
                self.__active = False

            items = api_response.items()

            if self.__max_results and (self.__loaded_results + len(items)) >= self.__max_results:
                self.__active = False
                num_of_loadable_results = self.__max_results - self.__loaded_results
                return items[0:num_of_loadable_results]
            else:
                self.__loaded_results += len(items)
                return items
