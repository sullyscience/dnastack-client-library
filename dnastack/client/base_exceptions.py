from typing import Any, List, Optional

from dnastack.feature_flags import detailed_error, in_global_debug_mode


class AmbiguousArgumentsError(AssertionError):
    """ Raised when two or more arguments for the same reason are provided. """


class UnauthenticatedApiAccessError(RuntimeError):
    """ Raised when the access to the API requires an authentication. """

    def __init__(self, message: str):
        super(UnauthenticatedApiAccessError, self).__init__(f'Unauthenticated Access: {message}')


class UnauthorizedApiAccessError(RuntimeError):
    """ Raised when the access to the API is denied. """

    def __init__(self, message: str):
        super(UnauthorizedApiAccessError, self).__init__(f'Unauthorized Access: {message}')


class MissingResourceError(RuntimeError):
    """ Raised when the requested resource is not found. """


class ServerApiError(RuntimeError):
    """ Raised when the server response """


class ApiError(RuntimeError):
    """ Raised when the server responds an error for unexpected reason. """

    def __init__(self, url: str, response_status: int, response_body: Any):
        super(ApiError, self).__init__(f'HTTP {response_status} from {url}: {response_body}')

        self.__url = url
        self.__status = response_status
        self.__details = response_body

    @property
    def url(self):
        return self.__url

    @property
    def status(self):
        return self.__status

    @property
    def details(self):
        return self.__details


class DataConnectError(RuntimeError):
    """ Raised when the server responds a HTTP-5xx error. """
    def __init__(self,
                 summary: str,
                 response_status: int,
                 details: Any = None,
                 urls: Optional[List[str]] = None,
                 url: Optional[str] = None):
        self.__urls = list()
        self.__status = response_status
        self.__summary = summary or details
        self.__details = details if self.__summary != details else None

        if urls:
            self.__urls.extend([url for url in urls if url])

        if url:
            self.__urls.append(url)

    @property
    def summary(self):
        return self.__summary

    @property
    def urls(self):
        return self.__urls

    @property
    def status(self):
        return self.__status

    @property
    def details(self):
        return self.__details

    def __str__(self):
        blocks = [f'HTTP {self.status}: {self.summary}' if self.summary else f'HTTP {self.status}']

        if in_global_debug_mode or detailed_error:
            if self.details:
                blocks.append(f'\nResponse Body:\n{self.details}')

            if self.urls:
                blocks.append(f'\nURL:')
                for url in self.__urls:
                    blocks.append(f' → {url}')

        return '\n'.join(blocks)