from typing import Optional, Type, List, Iterable

from imagination.decorator import service

from dnastack.client.constants import SERVICE_CLIENT_CLASS, ALL_SERVICE_CLIENT_CLASSES
from dnastack.client.models import ServiceEndpoint
from dnastack.client.service_registry.models import ServiceType
from dnastack.common.logger import get_logger
from dnastack.configuration.exceptions import MissingEndpointError
from dnastack.configuration.manager import ConfigurationManager
from dnastack.configuration.models import DEFAULT_CONTEXT
from dnastack.context.models import Context
from dnastack.configuration.wrapper import ConfigurationWrapper


class ServiceEndpointNotFound(RuntimeError):
    """ Raised when the requested service endpoint is not found """


class UnknownAdapterTypeError(RuntimeError):
    """ Raised when the given service adapter/short type is not registered or supported """


class NoServiceRegistryError(RuntimeError):
    """ Raised when there is no service registry to use """

    def __init__(self):
        super(NoServiceRegistryError, self).__init__('No service registry defined in the configuration')


class UnknownClientShortTypeError(RuntimeError):
    """ Raised when a given short service type is not recognized """


@service.registered()
class ConfigurationBasedClientFactory:
    """
    Configuration-based Client Factory

    This class will provide a service client based on the CLI configuration.
    """

    def __init__(self, config_manager: ConfigurationManager):
        self._config_manager = config_manager
        self._logger = get_logger(type(self).__name__)

    def get(self,
            cls: Type[SERVICE_CLIENT_CLASS],
            endpoint_id: Optional[str] = None,
            context_name: Optional[str] = None,
            **kwargs) -> SERVICE_CLIENT_CLASS:
        """
        Instantiate a service client with the given service endpoint.

        :param cls: The class (type) of the target service client, e.g., cls=DataConnectClient
        :param endpoint_id: The ID of the endpoint registered in the given context
        :param context_name: The name of the given context
        :param kwargs: Extra keyword arguments to the class factory method
        :return: an instance of the given class
        """
        context = self._get_context(context_name)
        return cls.make(self._get_endpoint(context, cls, endpoint_id), **kwargs)

    def _get_context(self, context_name: Optional[str]):
        config = self._config_manager.load()
        context_name = context_name or config.current_context

        assert context_name is not None, (
            f'The requested context ({context_name}) is not defined.'
        )

        if context_name == DEFAULT_CONTEXT and DEFAULT_CONTEXT not in config.contexts:
            config.contexts[DEFAULT_CONTEXT] = Context()
            self._config_manager.save(config)

        assert context_name in config.contexts, (
            f'The requested context ({context_name}) is defined but the context is not in the configuration file.'
            f' (contexts: {config.contexts.keys()})'
        )

        return config.contexts[context_name]

    def _get_endpoint(self,
                      context: Context,
                      cls: Type[SERVICE_CLIENT_CLASS],
                      endpoint_id: Optional[str] = None) -> ServiceEndpoint:
        supported_service_types = cls.get_supported_service_types()
        supported_service_type_list_in_string = " or ".join([str(t) for t in supported_service_types])

        endpoints = {
            endpoint.id: endpoint
            for endpoint in context.endpoints
            if endpoint.type in supported_service_types
        }

        if not endpoints:
            self._logger.error(f'Unable to find endpoints of type {supported_service_types} from {len(context.endpoints)} registered endpoint(s)')
            for endpoint in context.endpoints:
                self._logger.error(f' → {endpoint.type}: {endpoint.id}: {endpoint.url}')
            raise AssertionError(
                f'The selected context does not have any endpoints for {supported_service_type_list_in_string}.'
            )

        if endpoint_id:
            self._assert_with_alternative_ids(
                endpoint_id in endpoints,
                (
                    f'Endpoint "{endpoint_id}" is not defined for {supported_service_type_list_in_string} '
                    f'in this context.'
                ),
                endpoints.keys()
            )

            return endpoints[endpoint_id]
        else:
            short_type = cls.get_adapter_type()

            self._assert_with_alternative_ids(
                short_type in context.defaults and context.defaults[short_type],
                (
                    f'The default endpoint is not defined for {supported_service_type_list_in_string} '
                    f'in this context.'
                ),
                endpoints.keys()
            )

            default_endpoint_id = context.defaults[short_type]

            self._assert_with_alternative_ids(
                default_endpoint_id in endpoints,
                (
                    f'Endpoint "{default_endpoint_id}" is defined as the default endpoint for '
                    f'{supported_service_type_list_in_string} in this context.'
                ),
                endpoints.keys()
            )

            return endpoints[default_endpoint_id]

    def _assert_with_alternative_ids(self, condition: bool, error_message: str, alternative_ids: Iterable[str]):
        if not condition:
            id_list = '\n - '.join(sorted(alternative_ids))
            final_error_message = '\n'.join([
                error_message,
                f'',
                f'Try again with --endpoint-id=ENDPOINT where ENDPOINT is one of the following:\n - {id_list}.'
            ])
            raise AssertionError(final_error_message)

    @staticmethod
    def get_client_class(adapter_type: str) -> Type[SERVICE_CLIENT_CLASS]:
        for cls in ALL_SERVICE_CLIENT_CLASSES:
            if adapter_type == cls.get_adapter_type():
                return cls
        raise UnknownAdapterTypeError(adapter_type)

    @staticmethod
    def convert_from_short_type_to_full_types(short_type: str) -> List[ServiceType]:
        for client_class in ALL_SERVICE_CLIENT_CLASSES:
            if client_class.get_adapter_type() == short_type:
                return client_class.get_supported_service_types()
        raise UnknownClientShortTypeError(short_type)

    def get_default_endpoint(self,
                             adapter_type: str,
                             service_types: List[ServiceType]) -> Optional[ServiceEndpoint]:
        config = self._config_manager.load()
        wrapper = ConfigurationWrapper(config)

        if adapter_type in wrapper.defaults:
            try:
                return wrapper.get_endpoint(adapter_type=adapter_type,
                                            service_types=service_types,
                                            endpoint_id=wrapper.defaults[adapter_type])
            except MissingEndpointError:
                raise MissingEndpointError(f'No default endpoint for "{adapter_type}"')
        else:
            raise MissingEndpointError(f'No default endpoint for "{adapter_type}"')
