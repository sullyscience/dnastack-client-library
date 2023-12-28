from dataclasses import dataclass
from typing import Iterator, Dict, Optional

from dnastack.client.constants import DATA_SERVICE_CLIENT_CLASSES
from dnastack.client.models import ServiceEndpoint, EndpointSource
from dnastack.client.service_registry.client import ServiceRegistry, STANDARD_SERVICE_REGISTRY_TYPE_V1_0, \
    ServiceListingError
from dnastack.client.service_registry.factory import ClientFactory
from dnastack.client.service_registry.helper import parse_ga4gh_service_info
from dnastack.common.events import EventSource
from dnastack.common.logger import get_logger
from dnastack.context.models import Context
from dnastack.context.context_wraper import get_endpoint_by_id


class InvalidServiceRegistryError(RuntimeError):
    pass


class ServiceRegistryManager:
    def __init__(self,
                 context: Optional[Context] = None):
        self.__logger = get_logger(type(self).__name__)
        self.__events = EventSource(['endpoint-sync', 'config-update'], origin=self)

        assert context is not None, 'The context is undefined.'

        self.__context = context
        self.__in_isolation = False

    def in_isolation(self, flag: bool):
        self.__in_isolation = flag

    @property
    def events(self):
        return self.__events

    def get_endpoint_iterator(self) -> Iterator[ServiceEndpoint]:
        for endpoint in self.__context.endpoints:
            yield endpoint

    def get_registry_endpoint_iterator(self) -> Iterator[ServiceEndpoint]:
        for endpoint in self.get_endpoint_iterator():
            if endpoint.type not in ServiceRegistry.get_supported_service_types():
                continue
            yield endpoint

    def add_registry_and_import_endpoints(self, registry_endpoint_id: str, registry_url: str) -> Context:
        # When the endpoint ID already exists, throw an error.
        if get_endpoint_by_id(self.__context, registry_endpoint_id):
            raise EndpointAlreadyExists(f'id = {registry_endpoint_id}')

        # When the registry URL is registered, throw an error.
        identical_registry_endpoint_ids = [
            endpoint.id
            for endpoint in self.__context.endpoints
            if (endpoint.url == registry_url
                and endpoint.type == STANDARD_SERVICE_REGISTRY_TYPE_V1_0)
        ]
        if identical_registry_endpoint_ids:
            raise EndpointAlreadyExists(f'This URL ({registry_url}) has already been registered locally with the '
                                        f'following ID(s): {", ".join(identical_registry_endpoint_ids)}')

        # Now, create a new endpoint.
        registry_endpoint = ServiceEndpoint(id=registry_endpoint_id,
                                            url=registry_url,
                                            type=STANDARD_SERVICE_REGISTRY_TYPE_V1_0)

        # Check for endpoint usability
        try:
            for service_info in ServiceRegistry.make(registry_endpoint).list_services():
                self.__logger.debug(f'Detected Service: {service_info.id}')
        except ServiceListingError:
            raise InvalidServiceRegistryError(registry_url)

        # Add the registry endpoint.
        self.__context.endpoints.append(registry_endpoint)
        self.events.dispatch('endpoint-sync', dict(action='add', endpoint=registry_endpoint))

        # Initiate the first sync.
        return self.__synchronize_endpoints_with(ServiceRegistry.make(registry_endpoint))

    def synchronize_endpoints(self, registry_endpoint_id: str) -> Context:
        filtered_endpoints = [
            endpoint
            for endpoint in self.__context.endpoints
            if (endpoint.id == registry_endpoint_id
                and endpoint.type == STANDARD_SERVICE_REGISTRY_TYPE_V1_0)
        ]

        if not filtered_endpoints:
            raise RegistryNotFound(registry_endpoint_id)

        return self.__synchronize_endpoints_with(ServiceRegistry.make(filtered_endpoints[0]))

    def __synchronize_endpoints_with(self, registry: ServiceRegistry) -> Context:
        endpoints = self.__context.endpoints
        factory = ClientFactory([registry])

        sync_operations: Dict[str, _SyncOperation] = {
            endpoint.id: _SyncOperation(action='keep', endpoint=endpoint)
            for endpoint in endpoints
        }

        # Mark all advertised endpoints as new or updated endpoints.
        for service_entry in factory.all_service_infos():
            service_info = service_entry.info
            endpoint = parse_ga4gh_service_info(service_info,
                                                service_info.id if self.__in_isolation else f'{registry.endpoint.id}:{service_info.id}')
            endpoint.source = EndpointSource(source_id=registry.endpoint.id,
                                             external_id=service_info.id)
            sync_operations[endpoint.id] = _SyncOperation(action='update' if endpoint.id in sync_operations else 'add',
                                                          endpoint=endpoint)

        # Mark the existing associated endpoints for removal.
        for sync_operation in sync_operations.values():
            if not sync_operation.endpoint.source:
                continue

            if sync_operation.endpoint.source.source_id != registry.endpoint.id:
                continue

            if sync_operation.action != 'keep':
                continue

            sync_operation.action = 'remove'

        # Reconstruct the endpoint list.
        new_endpoint_list = []
        for sync_operation in sync_operations.values():
            endpoint = sync_operation.endpoint

            if sync_operation.action in ('add', 'update', 'keep'):
                new_endpoint_list.append(endpoint)

            self.events.dispatch('endpoint-sync', dict(action=sync_operation.action, endpoint=endpoint))

        endpoints.clear()
        endpoints.extend(sorted([endpoint for endpoint in new_endpoint_list], key=lambda e: e.id))

        # Set default for each type if not available.
        #
        # NOTE: When the code synchronizes with a service registry, if the default endpoint is not defined and
        #       there is only one endpoint of that type, then set that endpoint as default.
        #        - If there are more than one endpoint and the default is not set, don’t set it.
        #        - This only applies to when the code deals with a configuration object.
        for client_class in DATA_SERVICE_CLIENT_CLASSES:
            short_type = client_class.get_adapter_type()
            similar_endpoints = {
                endpoint.id: endpoint
                for endpoint in endpoints
                if endpoint.type in client_class.get_supported_service_types()
            }

            # Check if the default endpoint is still available.
            default_mapping = self.__context.defaults
            if short_type in default_mapping:
                default_endpoint_id = default_mapping[short_type]
                if default_endpoint_id in similar_endpoints:
                    self.__logger.info(f'The defined default "{short_type}" endpoint is still available '
                                       'and will be left untouched.')
                else:
                    del default_mapping[short_type]
                    self.__logger.debug(f'The defined default "{short_type}" endpoint is no longer available '
                                        'and has been unset.')

            if short_type not in default_mapping:
                if len(similar_endpoints) == 1:
                    default_mapping[short_type] = list(similar_endpoints.keys())[0]
                else:
                    self.__logger.info(f'The default "{short_type}" endpoint will not be set as there exists '
                                       f'{len(similar_endpoints)} endpoints ({", ".join([endpoint_id for endpoint_id in similar_endpoints])}).')

        return self.__context

    def remove_endpoints_associated_to(self, registry_endpoint_id: str) -> Context:
        endpoints = self.__context.endpoints

        new_endpoint_list = []

        for endpoint in endpoints:
            if (
                    endpoint.id == registry_endpoint_id
                    or (endpoint.source and endpoint.source.source_id == registry_endpoint_id)
            ):
                self.events.dispatch('endpoint-sync', dict(action='remove', endpoint=endpoint))
                continue
            else:
                new_endpoint_list.append(endpoint)
                self.events.dispatch('endpoint-sync', dict(action='keep', endpoint=endpoint))

        endpoints.clear()
        endpoints.extend(new_endpoint_list)

        return self.__context

    def list_endpoints_associated_to(self, registry_endpoint_id: str) -> Iterator[ServiceEndpoint]:
        for endpoint in self.__context.endpoints:
            if endpoint.source is not None and endpoint.source.source_id == registry_endpoint_id:
                yield endpoint


class RegistryNotFound(RuntimeError):
    def __init__(self, msg: str):
        super().__init__(msg)


class EndpointAlreadyExists(RuntimeError):
    def __init__(self, msg: str):
        super().__init__(msg)


@dataclass
class _SyncOperation:
    action: str
    endpoint: ServiceEndpoint
