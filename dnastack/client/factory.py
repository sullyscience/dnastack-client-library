from typing import Optional, Iterable, List, Type, Callable, Union, Dict

from dnastack.client.base_client import BaseServiceClient
from dnastack.client.constants import DATA_SERVICE_CLIENT_CLASSES, SERVICE_CLIENT_CLASS
from dnastack.client.models import ServiceEndpoint
from dnastack.client.service_registry.models import ServiceType
from dnastack.common.events import Event, EventHandler
from dnastack.common.logger import get_logger
from dnastack.common.simple_stream import SimpleStream


class UnsupportedServiceTypeError(RuntimeError):
    """ Raised when the given client class is not supported """

    def __init__(self, endpoint: ServiceEndpoint):
        super().__init__(f'{endpoint.id}: {endpoint.type.group}:{endpoint.type.artifact}:{endpoint.type.version} '
                         'is not supported')


def create(endpoint: ServiceEndpoint,
           additional_service_client_classes: Iterable[Type[BaseServiceClient]] = None) -> SERVICE_CLIENT_CLASS:
    supported_service_client_classes = list(DATA_SERVICE_CLIENT_CLASSES)
    if additional_service_client_classes:
        supported_service_client_classes.extend(additional_service_client_classes)
    for cls in supported_service_client_classes:
        if endpoint.type in cls.get_supported_service_types():
            return cls.make(endpoint)
    raise UnsupportedServiceTypeError(endpoint)


class SingleEndpointIdentifyingCriteriaError(RuntimeError):
    def __init__(self, endpoints: List[ServiceEndpoint], endpoint_type: Optional[ServiceType] = None,
                 client_class: Optional[Type[BaseServiceClient]] = None):
        super().__init__()
        self.endpoints = endpoints
        self.endpoint_type = endpoint_type
        self.client_class = client_class

    def __str__(self):
        condition: Optional[str] = None

        if self.endpoint_type:
            condition = f'endpoint_type = {self.endpoint_type}'
        elif self.client_class:
            condition = f'client_class = {self.client_class}'
        else:
            return 'No criteria specified'

        if len(self.endpoints) == 0:
            return f'No endpoints for {condition}'
        else:
            simplified_service_list = SimpleStream(self.endpoints)\
                .map(lambda endpoint: f'{endpoint.id} ({endpoint.type})')\
                .to_list()
            return f'Too many endpoints for {condition} (Endpoints: {simplified_service_list})'


class EndpointRepository:
    def __init__(self,
                 endpoints: Iterable[ServiceEndpoint],
                 cacheable=True,
                 additional_service_client_classes: Iterable[Type[BaseServiceClient]] = None,
                 default_event_interceptors: Optional[Dict[str, Union[EventHandler, Callable[[Event], None]]]] = None):
        self.__logger = get_logger(f'EndpointRepository/{hash(self)}')
        self.__cacheable = cacheable
        self.__endpoints = self.__set_endpoints(endpoints)
        self.__additional_service_client_classes = additional_service_client_classes
        self.__default_event_interceptors = default_event_interceptors or dict()

        self.__logger.debug('Initialized')

    def set_default_event_interceptors(self, interceptors: Dict[str, Union[EventHandler, Callable[[Event], None]]]):
        self.__default_event_interceptors.update(interceptors)
        for t in self.__default_event_interceptors:
            if self.__default_event_interceptors[t] is None:
                del self.__default_event_interceptors[t]

        self.__logger.debug(f'SET DEFAULT EVENT INTERCEPTORS: {self.__default_event_interceptors}')

    def all(self, *,
            endpoint_type: Optional[ServiceType] = None,
            client_class: Optional[Type[BaseServiceClient]] = None) -> List[ServiceEndpoint]:
        if endpoint_type is None and client_class is None:
            return self.__endpoints
        else:
            return SimpleStream(self.__endpoints)\
                .filter(lambda endpoint: self.__check_endpoint_compatibility(endpoint, endpoint_type, client_class))\
                .to_list()

    def get(self, id: str) -> Optional[SERVICE_CLIENT_CLASS]:
        for endpoint in self.__endpoints:
            if endpoint.id == id:
                return self.__create_client(endpoint)

        return None

    def get_one_of(self, *,
                   endpoint_type: Optional[ServiceType] = None,
                   client_class: Optional[Type[BaseServiceClient]] = None) -> SERVICE_CLIENT_CLASS:
        target_endpoints = self.all(endpoint_type=endpoint_type, client_class=client_class)

        if len(target_endpoints) == 1:
            return self.__create_client(target_endpoints[0])
        else:
            raise SingleEndpointIdentifyingCriteriaError(endpoints=target_endpoints,
                                                         endpoint_type=endpoint_type,
                                                         client_class=client_class)

    @staticmethod
    def __check_endpoint_compatibility(endpoint: ServiceEndpoint,
                                       endpoint_type: Optional[ServiceType] = None,
                                       client_class: Optional[Type[BaseServiceClient]] = None) -> bool:
        assert not (endpoint_type is None and client_class is None), 'One of the arguments MUST be specified.'

        if endpoint_type is not None and endpoint.type == endpoint_type:
            return True
        elif client_class is not None and endpoint.type in client_class.get_supported_service_types():
            return True
        else:
            return False

    def __create_client(self, endpoint: ServiceEndpoint) -> BaseServiceClient:
        client: BaseServiceClient = create(endpoint, self.__additional_service_client_classes)

        for event_type, event_handler in self.__default_event_interceptors.items():
            self.__logger.debug(f'{type(client).__name__}: SET EVENT HANDLER: {event_type} => {event_handler}')
            client.events.on(event_type, event_handler)

        return client

    def __set_endpoints(self, endpoints: Iterable[ServiceEndpoint]):
        return (
            [e for e in endpoints]
            if (self.__cacheable and not isinstance(endpoints, list))
            else endpoints
        )
