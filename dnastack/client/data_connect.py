import re
from dataclasses import dataclass
from datetime import datetime, time, date, timedelta
from decimal import Decimal
from pprint import pformat
from pydantic import BaseModel, ValidationError, Field
from requests import exceptions as requests_exc
from typing import Optional, Any, Dict, List, Iterator, Union, Callable, Iterable
from urllib.parse import urljoin

from dnastack.client.base_client import BaseServiceClient
from dnastack.client.base_exceptions import UnauthenticatedApiAccessError, UnauthorizedApiAccessError, \
    MissingResourceError, DataConnectError
from dnastack.client.result_iterator import ResultLoader, ResultIterator, InactiveLoaderError
from dnastack.client.service_registry.models import ServiceType
from dnastack.common.logger import get_logger
from dnastack.http.session import HttpSession, HttpError, ClientError

_logger = get_logger('module/data_connect')

DATA_CONNECT_TYPE_V1_0 = ServiceType(group='org.ga4gh',
                                     artifact='data-connect',
                                     version='1.0.0')


class InactiveQuerySessionError(InactiveLoaderError):
    """ Raised when the query loader has ended its session """


class TableNotFoundError(MissingResourceError):
    """ Raised when the requested table is not found """


class DataConversionError(RuntimeError):
    """ Raised when the data conversion fails """


class Error(BaseModel):
    """ Error representation """
    status: Any
    title: str
    details: Optional[str] = None

    def get_message(self) -> str:
        if self.details:
            return f'[{self.title}] {self.details}'
        else:
            return self.title


class TableInfo(BaseModel):
    """ Table metadata """
    name: str
    description: Optional[str] = None
    data_model: Optional[Dict[str, Any]] = Field(default_factory=dict)
    errors: Optional[List[Error]] = Field(default_factory=list)


class Pagination(BaseModel):
    """ Pagination """
    next_page_url: Optional[str] = None


class TableDataResponse(BaseModel):
    """ Table data """
    data: Optional[List[Dict[str, Any]]] = Field(default_factory=list)
    data_model: Optional[Dict[str, Any]] = Field(default_factory=list)
    pagination: Optional[Pagination] = None
    errors: Optional[List[Error]] = Field(default_factory=list)


class ListTablesResponse(BaseModel):
    """ Representation of the list of table """
    tables: Optional[List[TableInfo]] = Field(default_factory=list)
    pagination: Optional[Pagination] = None
    errors: Optional[List[Error]] = Field(default_factory=list)


class InterruptedLoadingError(RuntimeError):
    pass


class PaginableResultLoader(ResultLoader):
    def __init__(self,
                 initial_url: str,
                 http_session: Optional[HttpSession] = None):
        self._http_session = http_session
        self._initial_url = initial_url
        self._current_url: Optional[str] = None
        self._active = True
        self._visited_urls: List[str] = list()

    def _post_request(self, api_response: Union[ListTablesResponse, TableDataResponse]):
        if api_response.errors:
            extracted_errors = [e.title for e in api_response.errors]

            self._active = False

            if self._current_url:
                # The iterator encounters an unexpected error while iterating the result. Return an empty list.
                self.logger.warning(
                    f'While listing tables from {self._initial_url}, the server failed to respond to the request to '
                    f'{self._current_url} due to errors and the client will return the data received so far.'
                )
            else:
                # The iterator encounters an error on the first request.
                self.logger.error(f'The server responds an error while making a request to {self._initial_url}.')

            self.logger.warning(f'The errors are: {extracted_errors}')

            raise InterruptedLoadingError()

        if api_response.pagination:
            next_page_url = api_response.pagination.next_page_url

            if next_page_url and not re.search(r'https?://', next_page_url):
                self._current_url = urljoin(self._current_url or self._initial_url, next_page_url)
            else:
                self._current_url = next_page_url
        else:
            self._current_url = None

        if not self._current_url:
            self._active = False
        elif self._current_url in self._visited_urls:
            self._active = False


class TableListLoader(PaginableResultLoader):
    def load(self) -> List[TableInfo]:
        if not self._active:
            raise InactiveQuerySessionError(self._initial_url)

        with self._http_session as session:
            current_url = self._current_url or self._initial_url

            try:
                response = session.get(current_url)
            except HttpError as e:
                status_code = e.response.status_code
                response_text = e.response.text

                self._visited_urls.append(current_url)

                if status_code == 401:
                    raise UnauthenticatedApiAccessError(self.__generate_api_error_feedback(response_text))
                elif status_code == 403:
                    raise UnauthorizedApiAccessError(self.__generate_api_error_feedback(response_text))
                elif status_code >= 400:  # Catch all errors
                    raise DataConnectError(
                        f'Unexpected error: {response_text}',
                        status_code,
                        response_text,
                        urls=self._visited_urls
                    )

            status_code = response.status_code
            response_text = response.text

            try:
                response_body = response.json() if response_text else dict()
            except Exception:
                self.logger.error(f'{self._initial_url}: Unexpectedly non-JSON response body from {current_url}')
                raise DataConnectError(
                    f'Unable to deserialize JSON from {response_text}.',
                    status_code,
                    response_text,
                    urls=self._visited_urls
                )

            try:
                if isinstance(response_body, list):
                    api_response = ListTablesResponse(tables=response_body)
                else:
                    api_response = ListTablesResponse(**response_body)
            except ValidationError:
                raise DataConnectError(
                    f'Invalid Response Body: {response_body}',
                    status_code,
                    response_text,
                    urls=self._visited_urls
                )

            self.logger.debug(f'Response:\n{pformat(response_body, indent=2)}')

            try:
                self._post_request(api_response)
            except InterruptedLoadingError:
                return []

            return api_response.tables or []

    def has_more(self) -> bool:
        return self._active or self._current_url

    def __generate_api_error_feedback(self, response_body) -> str:
        if self._current_url:
            return f'Failed to load a follow-up page of the table list from {self._current_url} ({response_body})'
        else:
            return f'Failed to load the first page of the table list from {self._initial_url} ({response_body})'


@dataclass(frozen=True)
class DataMapper:
    str_pattern: re.Pattern
    map: Callable[[Any], Any]

    @classmethod
    def init(cls,
             str_pattern: re.Pattern,
             map: Callable[[Any], Any]):
        return cls(str_pattern, map)

    def can_handle(self, content: Any) -> bool:
        if isinstance(content, (str, bytes)):
            return self.str_pattern.match(content) is not None
        else:
            logger = get_logger(type(self).__name__)
            logger.warning(f'Unable to do pattern matching on:\n({type(content).__name__}) {content}')
            return True


class IntervalDayToSecondMapper(DataMapper):
    def __init__(self):
        super().__init__(
            re.compile(r'P((?P<days>\d+)D)?(T((?P<hours>\d+)H)?((?P<minutes>\d+)M)?((?P<seconds>\d+)S)?)?'),
            self._map
        )

    def _map(self, s: Any) -> Any:
        raw_delta = self.str_pattern.match(s).groupdict()
        return timedelta(**{
            p: int(v) if v is not None else 0
            for p, v in raw_delta.items()
        })


@dataclass(frozen=True)
class DataMapperGroup:
    json_type: str
    formats: List[str]
    mappers: List[DataMapper]

    def can_handle(self, given_json_types: List[str], given_data_format: str) -> bool:
        return self.json_type in given_json_types and given_data_format in self.formats

    def __str__(self):
        return f'{type(self).__name__}(json_type={self.json_type}, formats={self.formats})'


class InvalidQueryError(RuntimeError):
    """Invalid Query Error"""


class QueryLoader(PaginableResultLoader):
    _data_mapper_groups: Iterable[DataMapperGroup] = [
        # numbers
        DataMapperGroup(
            'string',
            ['bigint'],
            [
                DataMapper.init(
                    re.compile(r'^\d+$'),
                    lambda s: int(s)
                )
            ]
        ),
        DataMapperGroup(
            'string',
            ['decimal'],
            [
                DataMapper.init(
                    re.compile(r'^(\d+\.\d+|\d+|\.\d+)$'),
                    lambda s: Decimal(s)
                )
            ]
        ),
        # date
        DataMapperGroup(
            'string',
            ['date'],
            [
                DataMapper.init(
                    re.compile(r'^\d{4}-\d{2}-\d{2}$'),
                    lambda s: date.fromisoformat(s)
                ),
            ]
        ),
        # time without time zone
        DataMapperGroup(
            'string',
            ['time', 'time without time zone'],
            [
                DataMapper.init(
                    re.compile(r'^\d{2}:\d{2}:\d{2}(\.\d{,6})?$'),
                    lambda s: time.fromisoformat(s)
                ),
            ]
        ),
        # time with time zone
        DataMapperGroup(
            'string',
            ['time with time zone'],
            [
                DataMapper.init(
                    re.compile(r'^\d{2}:\d{2}:\d{2}(\.\d{,6})?Z$'),
                    lambda s: time.fromisoformat(s[:-1] + '+00:00')
                ),
                DataMapper.init(
                    re.compile(r'^\d{2}:\d{2}:\d{2}(\.\d{,6})?\s*(-|\+)\d{2}$'),
                    lambda s: time.fromisoformat(s + ':00')
                ),
                DataMapper.init(
                    re.compile(r'^\d{2}:\d{2}:\d{2}(\.\d{,6})?\s*(-|\+)\d{2}:\d{2}$'),
                    lambda s: time.fromisoformat(s)
                ),
            ]
        ),
        # timestamp without time zone
        DataMapperGroup(
            'string',
            ['timestamp', 'timestamp without time zone'],
            [
                DataMapper.init(
                    re.compile(r'^\d{4}-\d{2}-\d{2}(T| )\d{2}:\d{2}:\d{2}(\.\d{,6})?$'),
                    lambda s: datetime.fromisoformat(s)
                ),
            ]
        ),
        # timestamp with time zone
        DataMapperGroup(
            'string',
            ['timestamp with time zone'],
            [
                DataMapper.init(
                    re.compile(r'^\d{4}-\d{2}-\d{2}(T| )\d{2}:\d{2}:\d{2}(\.\d{,6})?Z$'),
                    lambda s: datetime.fromisoformat(s[:-1] + '+00:00')
                ),
                DataMapper.init(
                    re.compile(r'^\d{4}-\d{2}-\d{2}(T| )\d{2}:\d{2}:\d{2}(\.\d{,6})?(-|\+)\d{2}$'),
                    lambda s: datetime.fromisoformat(s + ':00')
                ),
                DataMapper.init(
                    re.compile(r'^\d{4}-\d{2}-\d{2}(T| )\d{2}:\d{2}:\d{2}(\.\d{,6})?(-|\+)\d{2}:\d{2}$'),
                    lambda s: datetime.fromisoformat(s)
                ),
            ]
        ),
        # NOTE: the "interval year to month" type is not supported.
        # interval day to second
        DataMapperGroup(
            'string',
            ['interval day to second'],
            [
                IntervalDayToSecondMapper()
            ]
        )
    ]

    def __init__(self,
                 initial_url: str,
                 query: Optional[str] = None,
                 http_session: Optional[HttpSession] = None):
        super(QueryLoader, self).__init__(initial_url=initial_url, http_session=http_session)

        self.__query = query
        self.__schema: Dict[str, Any] = dict()

    def load(self) -> List[Dict[str, Any]]:
        if not self._active:
            raise InactiveQuerySessionError(self._initial_url)

        with self._http_session as session:
            try:
                if not self._current_url:
                    # Load the initial page.
                    if self.__query:
                        # Send a search request
                        self.logger.debug(f'Initial Page: QUERY: {self._initial_url}: {self.__query}')
                        try:
                            response = session.post(self._initial_url, json=dict(query=self.__query))
                        except ClientError as e:
                            if e.response.status_code == 400:
                                feedback = e.response.text
                                raise InvalidQueryError(f'{self.__query}\n\nas the server responded with '
                                                        f'{feedback or "(empty response)"} ({len(feedback)} B).') from e
                            else:
                                raise e
                    else:
                        # Fetch the table data
                        self.logger.debug(f'Initial Page: URL: {self._initial_url}')
                        response = session.get(self._initial_url)
                    self._visited_urls.append(self._initial_url)
                else:
                    # Load a follow-up page.
                    self.logger.debug(f'Follow-up: URL: {self._current_url}')
                    response = session.get(self._current_url)
                    self._visited_urls.append(self._current_url)
            except HttpError as e:
                status_code = e.response.status_code
                response_body = e.response.text
                self.logger.debug(f'Response (JSON):\n{response_body}')

                if status_code == 401:
                    raise UnauthenticatedApiAccessError(self.__generate_api_error_feedback(response_body))
                elif status_code == 403:
                    raise UnauthorizedApiAccessError(self.__generate_api_error_feedback(response_body))
                elif status_code == 404:
                    raise TableNotFoundError(self.__generate_api_error_feedback(response_body))
                else:
                    # noinspection PyBroadException
                    try:
                        error_response = TableDataResponse(**e.response.json())
                        error_feedback = ', '.join([e.get_message() for e in error_response.errors])
                    except requests_exc.JSONDecodeError:
                        error_feedback = response_body
                    except:
                        error_feedback = response_body
                    raise DataConnectError(
                        error_feedback,
                        status_code,
                        response_body,
                        urls=self._visited_urls
                    )

            api_response = TableDataResponse(**response.json())

            try:
                self._post_request(api_response)
            except InterruptedLoadingError:
                return []

            if not self.__schema and api_response.data_model:
                self.__schema = api_response.data_model

            return self.__remap_array(self.__schema, api_response.data)

    def has_more(self) -> bool:
        return self._active or self._current_url

    def __remap_array(self, schema: Dict[str, Any], array: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not schema:
            return array
        else:
            return [self.__remap_obj(schema, row) for row in array]

    def __remap_obj(self, schema: Dict[str, Any], obj: Any) -> Any:
        if not schema:
            return obj

        obj_types = (
            ([schema['type']] if isinstance(schema['type'], str) else schema['type'])
            if 'type' in schema
            else ['object']
        )

        if obj is None:
            return None
        if 'array' in obj_types and isinstance(obj, (tuple, list)):
            return self.__remap_array(schema['items'], obj)
        elif 'object' in obj_types and isinstance(obj, dict):
            if schema.get('properties'):
                return {
                    property_name: (
                        self.__remap_obj(schema['properties'][property_name], property_value)
                        if property_name in schema['properties'] and schema['properties'][property_name]
                        else property_value
                    )
                    for property_name, property_value in obj.items()
                }
            else:
                return obj
        else:
            return self.__remap_value(obj_types, schema.get('format'), obj)

    def __remap_value(self, json_types: List[str], data_format: str, value: Any):
        # Source: https://github.com/ga4gh-discovery/data-connect/blob/develop/SPEC.md#correspondence-between-sql-and-json-data-types-in-the-search-result
        # NOTE: Non-standard data type will also not be handled and the original value will be returned.
        for mapper_group in self._data_mapper_groups:
            if not mapper_group.can_handle(json_types, data_format):
                continue

            mapper_index = 0
            for mapper in mapper_group.mappers:
                if not mapper.can_handle(value):
                    mapper_index += 1
                    continue

                if not mapper.map:
                    raise NotImplementedError(
                        f'The mapper #{mapper_index} is not fully implemented for {mapper_group.formats}.')

                try:
                    return mapper.map(value)
                except Exception:
                    raise DataConversionError(f'{mapper_group}#{mapper_index}: Unexpected error during data '
                                              f'conversion with {mapper.str_pattern.pattern}')

        return value

    def __generate_api_error_feedback(self, response_body=None) -> str:
        if self.__query:
            if self._current_url:
                return f'Failed to load a follow-up page of the result from this query:\n\n{self.__query}\n\nResponse:\n{response_body}'
            else:
                return f'Failed to load the first page of the result from this query:\n\n{self.__query}\n\nResponse:\n{response_body}'
        else:
            if self._current_url:
                return f'Failed to load a follow-up page of {self._current_url}'
            else:
                return f'Failed to load the first page of {self._initial_url}'


class Table:
    """ Table API Wrapper """

    def __init__(self,
                 table_name: str,
                 url: str,
                 http_session: Optional[HttpSession] = None):
        self.__http_session = http_session
        self.__table_name = table_name
        self.__url = url

    @property
    def name(self):
        """ The name of the table """
        return self.__table_name

    @property
    def info(self):
        """ The information of the table, such as schema """
        with self.__http_session as session:
            table_name = self.__table_name

            try:
                response = session.get(urljoin(self.__url, 'info'))
            except ClientError as e:
                status_code = e.response.status_code
                if status_code == 401:
                    raise UnauthenticatedApiAccessError('Authentication required')
                elif status_code == 403:
                    raise UnauthorizedApiAccessError('Insufficient privilege')
                elif status_code == 404:
                    raise TableNotFoundError(table_name)

            response_body = response.json()

            return TableInfo(**response_body)

    @property
    def data(self) -> Iterator[Dict[str, Any]]:
        """ The iterator to the data in the table """
        return ResultIterator(QueryLoader(http_session=self.__http_session,
                                          initial_url=urljoin(self.__url, 'data')))


class DataConnectClient(BaseServiceClient):
    """
    A Client for the GA4GH Data Connect standard
    """

    @staticmethod
    def get_adapter_type() -> str:
        return 'data_connect'

    @staticmethod
    def get_supported_service_types() -> List[ServiceType]:
        return [
            DATA_CONNECT_TYPE_V1_0,
        ]

    def query(self, query: str, no_auth: bool = False) -> Iterator[Dict[str, Any]]:
        """ Run an SQL query """
        return ResultIterator(QueryLoader(http_session=self.create_http_session(no_auth=no_auth),
                                          initial_url=urljoin(self.url, r'search'),
                                          query=query))

    def iterate_tables(self, no_auth: bool = False) -> Iterator[TableInfo]:
        """ Iterate the list of tables """
        return ResultIterator(TableListLoader(http_session=self.create_http_session(no_auth=no_auth),
                                              initial_url=urljoin(self.url, r'tables')))

    def list_tables(self, no_auth: bool = False) -> List[TableInfo]:
        """ List all tables """
        return [t for t in self.iterate_tables(no_auth=no_auth)]

    def table(self, table: Union[TableInfo, Table, str], no_auth: bool = False) -> Table:
        """ Get the table wrapper """
        table_name = self._get_table_name(table)
        table_url = urljoin(self.url, f'table/{table_name}/')
        return Table(http_session=self.create_http_session(no_auth=no_auth),
                     table_name=table_name,
                     url=table_url)

    @staticmethod
    def _get_table_name(table: Union[TableInfo, Table, str]) -> str:
        return table.name if hasattr(table, 'name') else table
