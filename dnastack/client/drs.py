import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from contextlib import AbstractContextManager
from datetime import datetime
from enum import Enum
from io import TextIOWrapper
from typing import Optional, List, Dict
from urllib.parse import urlparse, urljoin

import urllib3
from pydantic import BaseModel, Field

from .base_client import BaseServiceClient
from .models import ServiceEndpoint
from .service_registry.models import ServiceType
from ..common.events import Event
from ..common.logger import get_logger
from ..http.session import HttpSession, HttpError

DRS_TYPE_V1_1 = ServiceType(group='org.ga4gh', artifact='drs', version='1.1.0')


class MissingOptionalRequirementError(RuntimeError):
    """ Raised when a optional requirement is not available """


class InvalidFileStreamingResponse(RuntimeError):
    """ Raised when the response is invalid for file streaming """


class InvalidDrsUrlError(ValueError):
    """ Raised when the DRS URL is invalid """


class DrsApiError(RuntimeError):
    """ Raised when the DRS server responds an error """


class NoUsableAccessMethodError(RuntimeError):
    """ Raised when there is no usable access methods """


class DRSException(RuntimeError):
    def __init__(self, msg: str = None, url: str = None, object_id: str = None):
        self.msg = msg
        self.url = url
        self.object_id = object_id

    def __repr__(self):
        error_msg = "Failure downloading DRS object"
        if self.url:
            error_msg += f" with url [{self.url}]"
        elif self.object_id:
            error_msg += f" with object ID [{self.object_id}]"
        if self.msg:
            error_msg += f": {self.msg}"
        return error_msg

    def __str__(self):
        return str(self.__repr__())


class DRSDownloadException(RuntimeError):
    def __init__(self, errors: List[DRSException] = None):
        self.errors = errors

    def __repr__(self):
        error_msg = f"Downloads failed:\n"
        for err in self.errors:
            error_msg += f"{err}\n"
        return error_msg

    def __str__(self):
        return str(self.__repr__())


class DrsMinimalMetadata:
    """
    A class for a DRS resource

    :param url: The DRS url
    :raises ValueError if url is not a valid DRS url
    """

    __RE_VALID_DRS_OBJECT_ID = re.compile(r'^[^/#?]+$')

    def __init__(self, url: str):
        try:
            self.assert_valid_drs_url(url)
        except AssertionError:
            raise InvalidDrsUrlError(f"The provided url ({url}) is not a valid DRS url.")

        self.__url = url

    @property
    def url(self):
        return self.__url

    @property
    def object_id(self) -> str:
        """
        Return the object ID from a drs url
        """
        parsed_url = urlparse(self.url)
        return parsed_url.path.split("/")[-1]

    @property
    def drs_server_url(self) -> str:
        """
        Return the HTTPS server associated with the DRS url

        :param url: A drs url
        :return: The associated HTTPS server url
        """
        parsed_url = urlparse(self.url)
        return urljoin(f'https://{parsed_url.netloc}{"/".join(parsed_url.path.split("/")[:-1])}', 'ga4gh/drs/v1/')

    @classmethod
    def assert_valid_drs_url(cls, url: str):
        """Returns true if url is a valid DRS url"""
        parsed_url = urlparse(url)
        assert parsed_url.scheme == r'drs', \
            f'The scheme of the given URL ({url}) is invalid.'
        assert len(parsed_url.path) > 2 and parsed_url.path.startswith(r'/'), \
            f'The ID is not specified in the URL ({url}).'
        assert cls.__RE_VALID_DRS_OBJECT_ID.search(parsed_url.path[1:]), \
            f'The format of the ID ({parsed_url.path[1:]}) is not valid.'


class DrsObjectAccessUrl(BaseModel):
    headers: Optional[Dict[str, str]] = Field(default_factory=dict)
    url: str


class DrsObjectAccessMethod(BaseModel):
    access_id: Optional[str] = None
    access_url: Optional[DrsObjectAccessUrl] = None
    type: str


class DrsObjectChecksum(BaseModel):
    checksum: str
    type: str


class DrsObject(BaseModel):
    """
    This is based on https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.1.0/docs/#_drsobject.

    NOTE: This could be partial mapping.
    """
    id: str
    name: str
    access_methods: Optional[List[DrsObjectAccessMethod]] = Field(default_factory=list)
    checksums: List[DrsObjectChecksum]
    created_time: datetime
    updated_time: datetime
    size: int
    version: Optional[str] = None


class DownloadOkEvent(Event):
    @property
    def drs_url(self):
        return self.details.get('drs_url')

    @property
    def content(self):
        return self.details.get('content')

    @property
    def output_file_path(self):
        return self.details.get('output_file_path')

    @classmethod
    def make(cls, **kwargs):
        return cls(details=kwargs)


class DownloadProgressEvent(Event):
    @property
    def drs_url(self):
        return self.details.get('drs_url')

    @property
    def read_byte_count(self):
        return self.details.get('read_byte_count')

    @property
    def total_byte_count(self):
        return self.details.get('total_byte_count')

    @classmethod
    def make(cls, **kwargs):
        return cls(details=kwargs)


class DownloadFailureEvent(Event):
    @property
    def drs_url(self):
        return self.details.get('drs_url')

    @property
    def reason(self):
        return self.details.get('reason')

    @property
    def error(self):
        return self.details.get('error')

    @classmethod
    def make(cls, **kwargs):
        return cls(details=kwargs)


class DownloadStatus(Enum):
    """An Enum to Describe the current status of a DRS download"""

    SUCCESS = 0
    FAIL = 1


class Blob(AbstractContextManager):
    def __init__(self, drs_url: str, session: HttpSession):
        self._logger = get_logger(f'{type(self).__name__}/{drs_url}')
        self.__drs_url = drs_url
        self.__metadata = DrsMinimalMetadata(self.__drs_url)
        self.__object: Optional[DrsObject] = None
        self.__session = session
        self.__pool: Optional[urllib3.PoolManager] = None
        self.__connection: Optional[TextIOWrapper] = None
        self.__cache_data: Optional[bytes] = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        super().__exit__(exc_type, exc_val, exc_tb)

    @property
    def drs_url(self) -> str:
        return self.__drs_url

    @property
    def drs_object(self) -> DrsObject:
        if not self.__object:
            self.get_object()
        return self.__object

    @property
    def _pool(self):
        if not self.__pool:
            self.__pool = urllib3.PoolManager()
        return self.__pool

    @property
    def _connection(self) -> urllib3.HTTPResponse:
        if not self.__connection or self.__connection.closed:
            self.__connection = self._pool.request('GET', self.get_download_url(), preload_content=False)
        return self.__connection

    @property
    def data(self) -> bytes:
        if not self.__cache_data:
            self.__cache_data = self._connection.read()
            self.__connection.close()
        return self.__cache_data

    @property
    def name(self) -> str:
        return urlparse(self.get_download_url()).path.split(r'/')[-1]

    def close(self):
        if self.__connection and not self.__connection.closed:
            self.__connection.close()
        if self.__pool:
            self.__pool.clear()

    def get_object(self) -> DrsObject:
        """ Get the DRS Access URL Object """
        if self.__object:
            return self.__object
        else:
            api_url = urljoin(self.__metadata.drs_server_url, f'objects/{self.__metadata.object_id}')

            try:
                object_info_response = self.__session.get(api_url)
            except HttpError as e:
                object_info_status_code = e.response.status_code

                if object_info_status_code == 404:
                    raise DrsApiError(f'DRS object does not exist (HTTP 404 on {api_url})')
                elif object_info_status_code == 403:
                    raise DrsApiError(f'Access Denied (HTTP 403 on {api_url}')
                else:
                    raise DrsApiError("There was an error getting object info from the DRS Client")

            object_info = object_info_response.json()

            self.__object = DrsObject(**object_info)

            return self.__object

    def get_access_url_object(self) -> DrsObjectAccessUrl:
        """ Get the DRS Access URL Object """
        drs_obj = self.get_object()
        self._logger.debug(f'DRS Object:\n\n{drs_obj.json(indent=2)}\n')

        if drs_obj.access_methods:
            for access_method in drs_obj.access_methods:
                if access_method.access_url:
                    # if we have a direct access_url for the access_method, use that
                    return access_method.access_url
                elif access_method.access_id:
                    # try to use the access_id to get the download url
                    if access_method.type == 'https':
                        object_access_response = self.__session.get(
                            urljoin(self.__metadata.drs_server_url,
                                    f'objects/{self.__metadata.object_id}/access/{access_method.access_id}')
                        )
                        return DrsObjectAccessUrl(**object_access_response.json())
                    else:
                        continue

            # we couldn't find a download url, exit unsuccessful
            raise NoUsableAccessMethodError()
        else:
            raise NoUsableAccessMethodError()  # next page token, just return

    def get_download_url(self) -> str:
        """ Get the URL to download the DRS object """
        return self.get_access_url_object().url


class DrsClient(BaseServiceClient):
    """Client for Data Repository Service"""

    def __init__(self, endpoint: ServiceEndpoint):
        super().__init__(endpoint)

        # A lock to prevent race conditions on exit_codes objects
        self.__output_lock = threading.Lock()
        # lock to prevent race conditions for file output
        self.__exit_code_lock = threading.Lock()

        self._events.add_fixed_types('download-ok', 'download-progress', 'download-failure')

    @staticmethod
    def get_adapter_type():
        return 'drs'

    @staticmethod
    def get_supported_service_types() -> List[ServiceType]:
        return [
            DRS_TYPE_V1_1,
        ]

    def exit_download(self, url: str, status: DownloadStatus, message: str = "", exit_codes: dict = None) -> None:
        """
        Report a file download with a status and message

        :param url: The downloaded resource's url
        :param status: The reported status of the download
        :param message: A message describing the reason for setting the status
        :param exit_codes: A shared dict for all reports used by download_files
        """
        if exit_codes is not None:
            with self.__exit_code_lock:
                exit_codes[status][url] = message

    def get_blob(self,
                 id_or_url: Optional[str] = None,
                 id: Optional[str] = None,
                 url: Optional[str] = None,
                 no_auth: bool = False) -> Blob:
        assert id_or_url or id or url, 'Please at least specify either "id_or_url" (first argument), "id", or "url".'

        method_logger = get_logger(f'{self._logger.name}/get_blob')
        method_logger.debug('Invoked with (id_or_url={id_or_url}, id={id}, url={url}, no_auth={no_auth})')

        if id_or_url:
            method_logger.debug('Using implicit argument')
            if id_or_url.startswith('drs://'):
                method_logger.debug('Assume to be a DRS URL')
                # It is assumed to be a DRS URL.
                drs_url = id_or_url
            else:
                method_logger.debug('Assume to be a DRS ID')
                # It is assumed to be a DRS ID.
                parsed_base_url = urlparse(self.endpoint.url)
                drs_url = f'drs://{parsed_base_url.netloc}/{id_or_url}'
        elif id:
            method_logger.debug('Using explicit argument (id)')
            # This is an explicit option for directly using the given ID as DRS ID.
            parsed_base_url = urlparse(self.endpoint.url)
            drs_url = f'drs://{parsed_base_url.netloc}/{id}'
        else:
            method_logger.debug('Using explicit argument (url)')
            # This is an explicit option for directly using the given URL as DRS URL.
            drs_url = url

        return Blob(drs_url, self.create_http_session(no_auth=no_auth))

    def __download_file(
            self,
            drs_id_or_url: str,
            output_dir: str,
            exit_codes: Optional[dict] = None,
            no_auth: bool = False
    ) -> None:
        # TODO #182443607 Move this method to dnastack.cli.drs
        try:
            with self.get_blob(drs_id_or_url, no_auth=no_auth) as output:
                output_file_path = os.path.join(output_dir, output.name)
                output_connection = output._connection
                output_headers = output_connection.headers
                host_service = output_headers.get("Server") or 'Known'

                if output_connection.status != 200:
                    self._logger.error(f'Response/URL: {drs_id_or_url}')
                    self._logger.error(f'Response/Service: {host_service}')
                    self._logger.error(f'Response/Status: {output_connection.status}')
                    self._logger.error(f'Response/Body: {output_connection.read().decode()}')

                    raise InvalidFileStreamingResponse(
                        f'The server ({host_service}) responded with HTTP {output_connection.status}.'
                    )

                if 'Content-Length' not in output_headers:
                    self._logger.error(f'Response/URL: {drs_id_or_url}')
                    self._logger.error(f'Response/Service: {host_service}')
                    self._logger.error(f'Response/Status: {output_connection.status}')
                    self._logger.error(f'Response/Body: {output_connection.read().decode()}')

                    raise InvalidFileStreamingResponse(
                        f'The server ({host_service}) did not provide the length of the content. '
                        f'(headers = {output_headers})'
                    )

                with open(output_file_path, "wb+") as dest:
                    stream_size = int(output._connection.headers["Content-Length"])
                    read_byte_count = 0
                    for chunk in output._connection.stream(1024):
                        read_byte_count += len(chunk)
                        dest.write(chunk)
                        self._events.dispatch('download-progress',
                                              DownloadProgressEvent.make(drs_url=drs_id_or_url,
                                                                         read_byte_count=read_byte_count,
                                                                         total_byte_count=stream_size)
                                              )
                self._events.dispatch('download-progress',
                                      DownloadProgressEvent.make(drs_url=drs_id_or_url,
                                                                 read_byte_count=read_byte_count,
                                                                 total_byte_count=stream_size)
                                      )
                self._events.dispatch('download-ok',
                                      DownloadOkEvent.make(drs_url=output.drs_url,
                                                           output_file_path=output_file_path))

            self.exit_download(drs_id_or_url, DownloadStatus.SUCCESS, "Download Successful", exit_codes)
        except InvalidDrsUrlError as e:
            self._logger.info(f'failed to download from {drs_id_or_url}: {type(e).__name__}: {e}')
            self._events.dispatch('download-progress',
                                  DownloadProgressEvent.make(drs_url=drs_id_or_url,
                                                             read_byte_count=1,
                                                             total_byte_count=1)
                                  )
            self._events.dispatch('download-failure',
                                  DownloadFailureEvent.make(drs_url=drs_id_or_url,
                                                            reason='Invalid DRS URL'))
            self.exit_download(
                drs_id_or_url,
                DownloadStatus.FAIL,
                f"{type(e).__name__}: {e}",
                exit_codes,
            )
        except NoUsableAccessMethodError as e:
            self._logger.info(f'failed to download from {drs_id_or_url}: {type(e).__name__}: {e}')
            self._events.dispatch('download-failure',
                                  DownloadFailureEvent.make(drs_url=drs_id_or_url,
                                                            reason='No access method'))
            self.exit_download(
                drs_id_or_url,
                DownloadStatus.FAIL,
                f"{type(e).__name__}: {e}",
                exit_codes,
            )
        except DrsApiError as e:
            self._logger.info(f'failed to download from {drs_id_or_url}: {type(e).__name__}: {e}')
            self._events.dispatch('download-failure',
                                  DownloadFailureEvent.make(drs_url=drs_id_or_url,
                                                            reason='Unexpected error while communicating with DRS API',
                                                            error=e))
            self.exit_download(
                drs_id_or_url,
                DownloadStatus.FAIL,
                f"{type(e).__name__}: {e}",
                exit_codes,
            )
        except Exception as e:
            self._logger.info(f'failed to download from {drs_id_or_url}: {type(e).__name__}: {e}')
            self._events.dispatch('download-failure',
                                  DownloadFailureEvent.make(drs_url=drs_id_or_url,
                                                            reason='Unexpected error',
                                                            error=e))
            self.exit_download(
                drs_id_or_url,
                DownloadStatus.FAIL,
                f"{type(e).__name__}: {e}",
                exit_codes,
            )

    def _download_files(
            self,
            id_or_urls: List[str],
            output_dir: str = os.getcwd(),
            no_auth: bool = False
    ) -> None:
        # TODO #182443607 Move this method to dnastack.cli.drs
        exit_codes = {status: {} for status in DownloadStatus}
        unique_urls = set(id_or_urls)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Define the maximum number of workers, limited to the number of CPUs.
        max_worker_count = os.cpu_count()
        if max_worker_count < 2:
            max_worker_count = 2

        future_to_url_map: Dict[Future, str] = dict()

        with ThreadPoolExecutor(max_workers=max_worker_count) as pool:
            for url in unique_urls:
                future = pool.submit(
                    self.__download_file,
                    drs_id_or_url=url,
                    output_dir=output_dir,
                    exit_codes=exit_codes,
                    no_auth=no_auth
                )
                future_to_url_map[future] = url

        # Wait for all tasks to complete
        for future in as_completed(future_to_url_map.keys()):
            future.result()

        # at least one download failed, create exceptions
        failed_downloads = [
            DRSException(msg=msg, url=url)
            for url, msg in exit_codes.get(DownloadStatus.FAIL).items()
        ]

        if len(unique_urls) == len(failed_downloads):
            self._logger.error(f'All of {len(unique_urls)} download(s) failed unexpectedly')
            raise DRSDownloadException(failed_downloads)
        elif len(failed_downloads) > 0:
            self._logger.warning(f'{len(failed_downloads)} out of {len(unique_urls)} download(s) failed unexpectedly')
            index = 0
            for failed_download in failed_downloads:
                self._logger.warning(f'Failure #{index}: {failed_download}')
                index += 1
