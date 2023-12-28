import click
from imagination import container
from typing import Iterator

from dnastack.cli.helpers.command.decorator import command
from dnastack.cli.helpers.command.group import AliasedGroup
from dnastack.cli.helpers.exporter import to_json
from dnastack.cli.helpers.printer import echo_result
from dnastack.client.service_registry.manager import ServiceRegistryManager
from dnastack.common.events import Event
from dnastack.common.logger import get_logger
from dnastack.configuration.manager import ConfigurationManager
from dnastack.client.models import ServiceEndpoint
from dnastack.configuration.wrapper import ConfigurationWrapper
from dnastack.feature_flags import dev_mode


@click.group('registries', cls=AliasedGroup, aliases=['reg'], hidden=not dev_mode)
def registry_command_group():
    """ Manage service registries """
    # The design of the command structure is inspired by "git remote"


@command(registry_command_group, 'list')
def list_registries():
    """ List registered service registries """
    click.echo(to_json([
        endpoint.dict(exclude_none=True)
        for endpoint in ServiceRegistryCommandHandler().get_registry_endpoint_iterator()
    ]))


@command(registry_command_group)
def add(registry_endpoint_id: str, registry_url: str):
    """
    Add a new service registry to the configuration and import all endpoints registered with it.

    The local ID of each imported endpoint will be "<registry_endpoint_id>:<external_id>".

    If there exists at least ONE service endpoints from the given registry then, throw an error.

    If the registry URL is already registered, then throw an error.
    """
    ServiceRegistryCommandHandler().add_registry_and_import_endpoints(registry_endpoint_id, registry_url)
    click.secho('Import completed', fg='green')


@command(registry_command_group)
def remove(registry_endpoint_id: str):
    """
    Remove the entry of the service registry from the configuration and remove all endpoints registered with it.
    """
    ServiceRegistryCommandHandler().remove_endpoints_associated_to(registry_endpoint_id)
    click.secho('Removal completed', fg='green')


@command(registry_command_group)
def sync(registry_endpoint_id: str):
    """
    Synchronize the service endpoints associated to the given service registry.

    This command will add new endpoints, update existing ones, and/or remove endpoints that are no longer registered
    with the given service registry.
    """
    ServiceRegistryCommandHandler().synchronize_endpoints(registry_endpoint_id)
    click.secho('Synchronization completed', fg='green')


@command(registry_command_group)
def list_endpoints(registry_endpoint_id: str):
    """ List all service endpoints imported from given registry """
    click.echo(to_json([
        endpoint.dict(exclude_none=True)
        for endpoint in ServiceRegistryCommandHandler().list_endpoints_associated_to(registry_endpoint_id)
    ]))


class ServiceRegistryCommandHandler:
    __emoji_map = {
        'add': '+',
        'update': '●',
        'keep': 'o',
        'remove': 'x',
    }

    __output_color_map = {
        'add': 'green',
        'update': 'magenta',
        'keep': 'yellow',
        'remove': 'red',
    }

    def __init__(self):
        self.__logger = get_logger(type(self).__name__)
        self.__config_manager: ConfigurationManager = container.get(ConfigurationManager)
        self.__config = self.__config_manager.load()
        self.__manager = ServiceRegistryManager(context=ConfigurationWrapper(self.__config).current_context)
        self.__manager.events.on('endpoint-sync', self.__handle_sync_event)

    def get_endpoint_iterator(self) -> Iterator[ServiceEndpoint]:
        return self.__manager.get_endpoint_iterator()

    def get_registry_endpoint_iterator(self) -> Iterator[ServiceEndpoint]:
        return self.__manager.get_registry_endpoint_iterator()

    def add_registry_and_import_endpoints(self, registry_endpoint_id: str, registry_url: str):
        self.__manager.add_registry_and_import_endpoints(registry_endpoint_id, registry_url)
        self.__config_manager.save(self.__config)

    def synchronize_endpoints(self, registry_endpoint_id: str):
        self.__manager.synchronize_endpoints(registry_endpoint_id)
        self.__config_manager.save(self.__config)

    def remove_endpoints_associated_to(self, registry_endpoint_id: str):
        self.__manager.remove_endpoints_associated_to(registry_endpoint_id)
        self.__config_manager.save(self.__config)

    def list_endpoints_associated_to(self, registry_endpoint_id: str) -> Iterator[ServiceEndpoint]:
        return self.__manager.list_endpoints_associated_to(registry_endpoint_id)

    def __handle_sync_event(self, event: Event):
        action: str = event.details['action']
        endpoint: ServiceEndpoint = event.details['endpoint']

        echo_result(
            'Endpoint',
            self.__output_color_map[action],
            action,
            f'{endpoint.id} ({endpoint.type.group}:{endpoint.type.artifact}:{endpoint.type.version}) at {endpoint.url}',
            self.__emoji_map[action]
        )
