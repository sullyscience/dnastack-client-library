import re

import click
from imagination import container
from pydantic import BaseModel
from typing import Dict, List, Any, Optional, Type, Tuple

from dnastack.cli.helpers.iterator_printer import show_iterator
from dnastack.client.base_client import BaseServiceClient
from dnastack.client.service_registry.models import ServiceType
from dnastack.feature_flags import dev_mode
from dnastack.http.authenticators.oauth2_adapter.models import OAuth2Authentication
from dnastack.cli.helpers.command.decorator import command
from dnastack.cli.helpers.command.spec import ArgumentSpec, RESOURCE_OUTPUT_SPEC
from dnastack.cli.helpers.exporter import to_json
from dnastack.cli.helpers.printer import echo_header
from dnastack.client.constants import ALL_SERVICE_CLIENT_CLASSES, SERVICE_CLIENT_CLASS
from dnastack.client.models import ServiceEndpoint
from dnastack.common.logger import get_logger
from dnastack.common.simple_stream import SimpleStream
from dnastack.configuration.manager import ConfigurationManager
from dnastack.context.models import Context
from dnastack.configuration.wrapper import ConfigurationWrapper, UnknownContextError
from dnastack.cli.helpers.client_factory import ConfigurationBasedClientFactory
from dnastack.json_path import JsonPath, BrokenPropertyPathError


@click.group('endpoints', hidden=not dev_mode)
def endpoint_command_group():
    """ Manage service endpoints """


@command(endpoint_command_group, 'available-properties')
def list_available_properties():
    """ List all available configuration property """
    config_properties = EndpointCommandHandler().list_available_properties()

    echo_header('General Configuration')

    SimpleStream(config_properties) \
        .filter(lambda p: not p.startswith('authentication.')) \
        .for_each(__show_config_property)

    echo_header('Authentication Configuration')

    # List the authentication info properties.
    # NOTE: While we still support the PAT flow, we are not advertising that method.
    SimpleStream(config_properties) \
        .filter(lambda p: p.startswith('authentication.')) \
        .filter(lambda p: not p.startswith('authentication.personal_access_')) \
        .for_each(__show_config_property)


def __show_config_property(config_property: str):
    click.secho('● ', dim=False, fg='blue', nl=False)
    click.secho(config_property)

    example_config_value = '...'
    if config_property.endswith('url') or config_property.endswith('endpoint'):
        example_config_value = 'https://...'
    if config_property.endswith('version'):
        example_config_value = '1.23.456'

    click.secho('  Examples:', dim=True, bold=True)
    click.secho(f'   ● dnastack endpoints get <ID> {config_property}', dim=True)
    click.secho(f'   ● dnastack endpoints set <ID> {config_property} "{example_config_value}"', dim=True)
    click.secho(f'   ● dnastack endpoints unset <ID> {config_property}', dim=True)
    print()


@command(endpoint_command_group, 'schema')
def show_schema():
    """
    Show the schema of the endpoint configuration

    This is mainly for development.
    """
    echo_header('Service Endpoint')
    click.echo(to_json(ServiceEndpoint.schema()))

    echo_header('OAuth2 Authentication Information')
    click.echo(to_json(OAuth2Authentication.schema()))


@command(endpoint_command_group,
         'list',
         specs=[
             ArgumentSpec(
                 name='short_or_full_type',
                 arg_names=['-t', '--type'],
                 as_option=True,
                 help='Short of full service type, e.g., "data_connect" or "org.ga4gh:data-connect:1.0"'
             ),
             RESOURCE_OUTPUT_SPEC,
         ])
def list_endpoints(context: Optional[str], short_or_full_type: Optional[str] = None, output: Optional[str] = None):
    """ List all registered service endpoint """
    handler = EndpointCommandHandler(context_name=context)
    full_type = handler.parse_given_service_type(short_or_full_type)[1] if short_or_full_type else None
    show_iterator(output_format=output, iterator=[
        endpoint.dict(exclude_none=True)
        for endpoint in handler.list_endpoints()
        if not full_type or endpoint.type == full_type
    ])


@command(endpoint_command_group,
         'add',
         specs=[
             ArgumentSpec(
                 name='service_type',
                 arg_names=['-t', '--type'],
                 help=(f'The short type ({", ".join([t.get_adapter_type() for t in ALL_SERVICE_CLIENT_CLASSES])}) '
                       'or the full type, e.g., org.ga4gh:wes:1.0, of the service endpoint'),
                 as_option=True,
             ),
         ])
def add_endpoint(context: Optional[str], id: str, service_type: str):
    """
    Add a new endpoint
    
    The argument SERVICE_TYPE is either:

     - the short type, e.g., collections, data_connect, drs, and registry,

     - or the full type, e.g., "org.ga4gh:wes:1.0"

    of the service endpoint.
    """
    EndpointCommandHandler(context_name=context).add_endpoint(id, service_type)


@command(endpoint_command_group, 'remove')
def remove_endpoint(context: Optional[str], id: str):
    """ Remove an endpoint """
    EndpointCommandHandler(context_name=context).remove_endpoint(id)


@command(endpoint_command_group, 'set')
def set_endpoint_property(context: Optional[str], id: str, config_property: str, config_value: str):
    """ Set the endpoint property """
    EndpointCommandHandler(context_name=context).set_endpoint_property(id, config_property, config_value)


@command(endpoint_command_group, 'unset')
def unset_endpoint_property(context: Optional[str], id: str, config_property: str):
    """ Unset the endpoint property """
    EndpointCommandHandler(context_name=context).set_endpoint_property(id, config_property, None)


@command(endpoint_command_group)
def get_defaults(context: Optional[str]):
    """ Get the command-group-to-default-endpoint-id map """
    click.echo(to_json(EndpointCommandHandler(context_name=context).get_defaults()))


@command(endpoint_command_group)
def set_default(context: Optional[str], id: str):
    """ Set the given endpoint as the default for its type """
    EndpointCommandHandler(context_name=context).set_default(id)


@command(endpoint_command_group)
def unset_default(context: Optional[str], id: str):
    """ Unset the given endpoint as the default for its type """
    EndpointCommandHandler(context_name=context).unset_default(id)


class EndpointCommandHandler:
    def __init__(self, context_name: Optional[str] = None):
        self.__logger = get_logger(type(self).__name__)
        self.__schema: Dict[str, Any] = self.__resolve_json_reference(ServiceEndpoint.schema())
        self.__config_manager: ConfigurationManager = container.get(ConfigurationManager)
        self.__config = self.__config_manager.load()
        self.__context_name = context_name
        self.__wrapper = ConfigurationWrapper(self.__config, context_name)

    def get_defaults(self):
        return self.__wrapper.defaults

    def set_default(self, id: str):
        wrapper = self.__wrapper
        context = wrapper.current_context

        endpoint = wrapper.get_endpoint_by_id(id)
        if not endpoint:
            raise EndpointNotFound(id)

        context.defaults[self.__get_short_type(endpoint)] = id

        self.__config_manager.save(wrapper.original)

    def unset_default(self, id: str):
        wrapper = self.__wrapper
        context = wrapper.current_context

        endpoint = wrapper.get_endpoint_by_id(id)
        if not endpoint:
            raise EndpointNotFound(id)

        del context.defaults[self.__get_short_type(endpoint)]

        self.__config_manager.save(self.__wrapper.original)

    def reset(self):
        # Reset v4 model
        self.__wrapper.defaults.clear()
        self.__wrapper.endpoints.clear()
        # Reset v3 model
        self.__wrapper.original.defaults.clear()
        self.__wrapper.original.endpoints.clear()
        # Save the changes
        self.__config_manager.save(self.__wrapper.original)

    def list_available_properties(self) -> List[str]:
        """ List all available configuration property """
        return SimpleStream(self.__list_all_json_path(self.__schema)) \
            .filter(lambda path: path not in ['id', 'adapter_type', 'mode', 'model_version']) \
            .to_list()

    def list_endpoints(self):
        """ List all registered service endpoint """
        return self.__wrapper.endpoints

    def parse_given_service_type(self, short_or_full_type: str) -> Tuple[Optional[str], Optional[ServiceType]]:
        short_type: Optional[str] = None
        full_type: Optional[ServiceType] = None

        if re.search(r'^[^:]+:[^:]+:[^:]+$', short_or_full_type):
            # This is the string representation of the full type.
            group, artifact, version = short_or_full_type.split(':')
            full_type = ServiceType(group=group, artifact=artifact, version=version)
        else:
            # This is the short type.
            for client_class in ALL_SERVICE_CLIENT_CLASSES:
                # Check against the short type.
                if client_class.get_adapter_type() == short_or_full_type:
                    short_type = client_class.get_adapter_type()
                    full_type = client_class.get_default_service_type()
                    break

            if not full_type:
                raise RuntimeError(f'The given type "{short_or_full_type}" is not recognized. Perhaps, you can try with'
                                   ' the full service type (<group>:<artifact>:<version>), e.g., "org.ga4gh:wes:1.0"')

        return (short_type, full_type)

    def add_endpoint(self, id: str, short_or_full_type: str):
        """ Add a new endpoint """
        short_type, full_type = self.parse_given_service_type(short_or_full_type)

        wrapper = self.__wrapper

        # Get the target context.
        target_context_name = self.__config.current_context
        context = self.__config.contexts.get(target_context_name)
        if context is None:
            context = self.__config.contexts[target_context_name] = Context()

        # Check the existing endpoint.
        first_existing_endpoint = wrapper.get_endpoint_by_id(id)
        if first_existing_endpoint:
            raise EndpointAlreadyExists(f'{id} ({first_existing_endpoint.type})')

        # Add a new endpoint with the first full service type.
        context.endpoints.append(ServiceEndpoint(id=id, type=full_type, url=''))

        # Set the default if the default is not defined.
        if short_type and short_type not in context.defaults:
            context.defaults[short_type] = id

        # Save the configuration
        self.__config_manager.save(self.__config)

    def set_endpoint_property(self, id: str, config_property: str, config_value: Optional[str]):
        """ Set the endpoint property """
        wrapper = self.__wrapper

        # Get the target context.
        target_context_name = self.__config.current_context
        context = self.__config.contexts.get(target_context_name)
        if context is None:
            raise UnknownContextError(target_context_name)

        endpoint = wrapper.get_endpoint_by_id(id)

        if endpoint is None:
            raise EndpointNotFound(id)

        if config_property not in self.list_available_properties():
            raise InvalidConfigurationProperty(config_property)

        try:
            JsonPath.set(endpoint, config_property, config_value)
        except BrokenPropertyPathError as __:
            self.__logger.debug(f'set_endpoint: BROKEN PATH: endpoint => {endpoint}')
            self.__logger.debug(f'set_endpoint: BROKEN PATH: config_property => {config_property}')
            # Attempt to repair the broken config_property.
            parent_path = '.'.join(config_property.split('.')[:-1])
            self.__repair_path(endpoint, parent_path)

            # Then, try again.
            JsonPath.set(endpoint, config_property, config_value)

        # Save the configuration
        self.__config_manager.save(self.__config)

    def remove_endpoint(self, id: str):
        """ Remove an endpoint """
        # Get the target context.
        target_context_name = self.__config.current_context
        context = self.__config.contexts.get(target_context_name)
        if context is None:
            raise UnknownContextError(target_context_name)

        # Recreate the list by excluding the one with the given ID.
        context.endpoints = SimpleStream(context.endpoints).filter(lambda e: e.id != id).to_list()

        # Remove the endpoint from the default endpoint map.
        for short_type in list(context.defaults.keys()):
            endpoint_id = context.defaults[short_type]
            if endpoint_id == id:
                del context.defaults[short_type]

        # Save the configuration
        self.__config_manager.save(self.__config)

    @staticmethod
    def __convert_from_short_type_to_full_types(short_type: str) -> List[ServiceType]:
        for client_class in ALL_SERVICE_CLIENT_CLASSES:
            if client_class.get_adapter_type() == short_type:
                return client_class.get_supported_service_types()
        raise None

    def __repair_path(self, obj, path: str, overridden_path_defaults: Dict[str, Any] = None):
        overridden_path_defaults = overridden_path_defaults or dict()

        selectors = path.split(r'.')
        visited = []

        self.__logger.debug(f'__repair_path: ENTER: type(obj) => {type(obj).__name__}')
        self.__logger.debug(f'__repair_path: ENTER: obj => {obj}')
        self.__logger.debug(f'__repair_path: ENTER: path => {path}')

        for selector in selectors:
            visited.append(selector)
            route = '.'.join(visited)

            self.__logger.debug(f'__repair_path: LOOP: route = {route}')

            try:
                JsonPath.get(obj, route, raise_error_on_null=True)
                break
            except BrokenPropertyPathError as e:
                visited_nodes = e.visited_path.split(r'.')
                last_visited_node = visited_nodes[-1]

                node = e.parent or obj

                self.__logger.debug(f'__repair_path: LOOP: ***** Broken Path Detected *****')
                self.__logger.debug(f'__repair_path: LOOP: type(e.parent) => {type(e.parent).__name__}')
                self.__logger.debug(f'__repair_path: LOOP: e.parent => {e.parent}')
                self.__logger.debug(f'__repair_path: LOOP: last_visited_node => {last_visited_node}')

                annotation = node.__annotations__[last_visited_node]

                if hasattr(node, last_visited_node) and getattr(node, last_visited_node):
                    self.__logger.debug(f'__repair_path: LOOP: No repair')
                elif str(annotation).startswith('typing.Union[') or str(annotation).startswith("typing.Optional["):
                    # Dealing with Union/Optional
                    self.__logger.debug(f'__repair_path: LOOP: Handling union and optional')
                    self.__logger.debug(f'__repair_path: LOOP: annotation.__args__ => {annotation.__args__}')
                    self.__initialize_default_value(node, last_visited_node, annotation.__args__[0])
                else:
                    self.__initialize_default_value(node, last_visited_node, annotation)

                self.__logger.debug(f'__repair_path: LOOP: node = {getattr(node, last_visited_node)}')

        if path in overridden_path_defaults:
            JsonPath.set(obj, path, overridden_path_defaults.get(path))

        self.__logger.debug(f'__repair_path: EXIT: obj => {obj}')

    def __initialize_default_value(self, node, property_name: str, annotation):
        if hasattr(node, property_name) and getattr(node, property_name) is not None:
            return
        elif str(annotation).startswith('typing.Dict['):
            setattr(node, property_name, dict())
        elif str(annotation).startswith('typing.List['):
            setattr(node, property_name, list())
        elif issubclass(annotation, BaseModel):
            required_properties = annotation.schema().get('required') or []
            placeholders = {
                p: self.__get_place_holder(annotation.__annotations__[p])
                for p in required_properties
            }
            setattr(node, property_name, annotation(**placeholders))
        else:
            setattr(node, property_name, annotation())

    def __get_place_holder(self, cls):
        if cls == str:
            return ''
        elif cls == int or cls == float:
            return 0
        elif cls == bool:
            return False
        else:
            raise NotImplementedError(cls)

    def __list_all_json_path(self, obj: Dict[str, Any], prefix_path: List[str] = None) -> List[str]:
        properties = obj.get('properties') or dict()
        paths = []

        prefix_path = prefix_path or list()

        if len(prefix_path) == 1 and prefix_path[0] == 'authentication':
            return [
                f'{prefix_path[0]}.{oauth2_path}'
                for oauth2_path in self.__list_all_json_path(OAuth2Authentication.schema())
            ]
        else:
            if obj['type'] == 'object':
                for property_name, obj_property in properties.items():
                    if 'anyOf' in obj_property:
                        for property_to_resolve in obj_property['anyOf']:
                            paths.extend(
                                self.__list_all_json_path(
                                    self.__fetch_json_reference(
                                        property_to_resolve['$ref'],
                                        self.__schema
                                    ),
                                    prefix_path + [property_name]
                                )
                            )
                    elif obj_property['type'] == 'object':
                        paths.extend(
                            self.__list_all_json_path(
                                obj_property,
                                prefix_path + [property_name]
                            )
                        )
                    elif obj_property['type'] == 'array':
                        paths.extend(
                            self.__list_all_json_path(
                                obj_property['items'],
                                prefix_path + [property_name]
                            )
                        )
                        paths.extend(
                            self.__list_all_json_path(
                                obj_property['items'],
                                prefix_path + [property_name + '[i]']
                            )
                        )
                    else:
                        prefix_path_string = '.'.join(prefix_path)
                        paths.append(f'{prefix_path_string}{"." if prefix_path_string else ""}{property_name}')

        return sorted(paths)

    def __resolve_json_reference(self, obj: Dict[str, Any], root: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        root = root or obj
        properties = obj.get('properties') or dict()
        for property_name, obj_property in properties.items():
            if obj_property.get('$ref'):
                properties[property_name] = self.__fetch_json_reference(obj_property.get('$ref'), root)
            # Deal with array
            if obj_property.get('items') and obj_property.get('items').get('$ref'):
                obj_property['items'] = self.__fetch_json_reference(obj_property.get('items').get('$ref'), root)

        return obj

    def __fetch_json_reference(self, reference_url: str, root: Dict[str, Any]):
        if reference_url.startswith('#/'):
            ref_path = reference_url[2:].split(r'/')
            local_reference = root
            try:
                while ref_path:
                    property_name = ref_path.pop(0)
                    local_reference = local_reference[property_name]
            except KeyError as e:
                raise RuntimeError(f'The reference {reference_url} for the configuration is undefined.')
            return self.__resolve_json_reference(local_reference, root)

        raise NotImplementedError('Resolving an external reference is not supported.')

    def __get_short_type(self, endpoint: ServiceEndpoint) -> str:
        def filter_by_type(cls: SERVICE_CLIENT_CLASS) -> bool:
            if endpoint.dnastack_schema_version == 1.0:
                return endpoint.adapter_type == cls.get_adapter_type()
            elif endpoint.dnastack_schema_version == 2.0:
                return endpoint.type in cls.get_supported_service_types()
            else:
                raise RuntimeError(f'The endpoint {endpoint.id} with model version {endpoint.dnastack_schema_version} is not '
                                   f'supported. Please check your configuration or contact technical support.')

        return SimpleStream(ALL_SERVICE_CLIENT_CLASSES) \
            .filter(filter_by_type) \
            .map(lambda c: c.get_adapter_type()) \
            .find_first()


class EndpointNotFound(RuntimeError):
    def __init__(self, msg: str):
        super().__init__(msg)


class EndpointAlreadyExists(RuntimeError):
    def __init__(self, msg: str):
        super().__init__(msg)


class InvalidConfigurationProperty(RuntimeError):
    def __init__(self, msg: str):
        super().__init__(msg)
