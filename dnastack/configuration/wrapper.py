import sys
from typing import List, Optional

from dnastack import CollectionServiceClient, DataConnectClient, DrsClient
from dnastack.client.collections.client import EXPLORER_COLLECTION_SERVICE_TYPE_V1_0, \
    STANDARD_COLLECTION_SERVICE_TYPE_V1_0
from dnastack.client.data_connect import DATA_CONNECT_TYPE_V1_0
from dnastack.client.drs import DRS_TYPE_V1_1
from dnastack.client.models import ServiceEndpoint
from dnastack.client.service_registry.models import ServiceType
from dnastack.common.logger import get_logger
from dnastack.common.simple_stream import SimpleStream
from dnastack.configuration.exceptions import MissingEndpointError
from dnastack.configuration.models import Configuration, DEFAULT_CONTEXT
from dnastack.context.models import Context
from dnastack.feature_flags import in_global_debug_mode


class UnsupportedModelVersionError(RuntimeError):
    pass


class UnknownContextError(RuntimeError):
    pass


class ConfigurationWrapper:
    _logger = get_logger('Configuration')

    def __init__(self, configuration: Configuration, context_name: Optional[str] = None):
        self.__config = configuration
        self.__context_name = context_name or self.__config.current_context

    @property
    def original(self):
        return self.__config

    @property
    def current_context(self) -> Context:
        # Special case: the default context
        context = self.__config.contexts.get(self.__context_name)

        # Special case: If the requested context is the default context, and it does not exist, just create one.
        if self.__context_name == DEFAULT_CONTEXT and context is None:
            context = self.__config.contexts[self.__context_name] = Context()

        return context

    @property
    def endpoints(self):
        context = self.current_context
        if context:
            return context.endpoints
        else:
            raise UnknownContextError(self.__context_name)

    @property
    def defaults(self):
        context = self.current_context
        if context:
            return context.defaults
        else:
            raise UnknownContextError(self.__context_name)

    def get_endpoint_by_id(self, id: str) -> ServiceEndpoint:
        return SimpleStream(self.current_context.endpoints).filter(lambda e: e.id == id).find_first()

    def _get_all_endpoints_by(self,
                              adapter_type: Optional[str] = None,
                              service_types: List[ServiceType] = None,
                              endpoint_id: Optional[str] = None) -> List[ServiceEndpoint]:
        endpoints = []

        for endpoint in self.endpoints:
            # If the ID is specified, the other conditions will be ignored.
            if endpoint_id:
                if endpoint.id == endpoint_id:
                    endpoints.append(endpoint)
                    self.__debug_message(f'_get_all_endpoints_by: E/{endpoint.id}: HIT (endpoint.id)')
                continue
            else:
                if endpoint.dnastack_schema_version == 2.0:
                    if service_types and endpoint.type not in service_types:
                        self.__debug_message(f'_get_all_endpoints_by: E/{endpoint.id}: MISSED: type is not matched')
                        continue
                elif endpoint.dnastack_schema_version == 1.0:
                    if adapter_type and endpoint.adapter_type != adapter_type:
                        self.__debug_message(f'_get_all_endpoints_by: E/{endpoint.id}: MISSED: adapter_type is not matched')
                        continue
                else:
                    raise UnsupportedModelVersionError(f'{type(endpoint).__name__}/{endpoint.dnastack_schema_version}')

                self.__debug_message(f'_get_all_endpoints_by: E/{endpoint.id}: HIT')
                endpoints.append(endpoint)

        return endpoints

    def get_endpoint(self,
                     adapter_type: str,
                     service_types: List[ServiceType],
                     endpoint_id: Optional[str] = None) -> ServiceEndpoint:
        endpoints: List[ServiceEndpoint] = self._get_all_endpoints_by(adapter_type, service_types, endpoint_id)
        endpoint: Optional[ServiceEndpoint] = endpoints[0] if endpoints else None

        # When the endpoint is not available...
        if endpoint is None:
            raise MissingEndpointError(f'The "{adapter_type}" endpoint #{endpoint_id or "?"} is not defined.')

        return endpoint

    def __debug_message(self, msg: str):
        if in_global_debug_mode and 'unittest' in sys.modules:
            sys.stderr.write(msg + '\n')
            sys.stderr.flush()
        else:
            self._logger.debug(msg)
