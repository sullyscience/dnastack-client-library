import click
import logging
import re
from imagination import container
from typing import Optional

from imagination.decorator.service import Service

from dnastack.cli.auth.event_handlers import handle_auth_begin, handle_auth_end, handle_no_refresh_token, \
    handle_refresh_skipped
from dnastack.cli.helpers.command.decorator import command
from dnastack.cli.helpers.command.spec import ArgumentSpec, RESOURCE_OUTPUT_SPEC
from dnastack.cli.helpers.iterator_printer import show_iterator
from dnastack.cli.helpers.printer import echo_result, echo_dict_in_table
from dnastack.client.models import ServiceEndpoint
from dnastack.common.events import Event
from dnastack.common.logger import get_logger
from dnastack.context.manager import ContextManager
from dnastack.feature_flags import dev_mode
from dnastack.http.authenticators.abstract import AuthState, AuthStateStatus


@click.group("contexts", hidden=not dev_mode)
def context_command_group():
    """
    Manage contexts
    """


@command(context_command_group,
         specs=[
             ArgumentSpec(
                 name='context_name',
                 arg_names=['--name'],
                 as_option=True,
                 help='Context name -- default to hostname'
             )
         ])
def use(hostname: str, context_name: Optional[str] = None, no_auth: bool = False):
    """
    Import a configuration from host's service registry (if available) or the corresponding public configuration from
    cloud storage. If "--no-auth" is not defined, it will automatically initiate all authentication.

    This will also switch the default context to the given hostname.
    """
    handler: ContextCommandHandler = container.get(ContextCommandHandler)
    handler.use(hostname, context_name=context_name, no_auth=no_auth)


@command(context_command_group)
def add(context_name: str):
    """ Add a context """
    try:
        handler: ContextCommandHandler = container.get(ContextCommandHandler)
        handler.manager.add(context_name)
        echo_result('Context', 'green', 'add', f'{context_name}')
    except AssertionError as e:
        echo_result('Context', 'yellow', 'aborted', str(e))
        exit(1)


@command(context_command_group)
def remove(context_name: str):
    """ Remove a context """
    try:
        handler: ContextCommandHandler = container.get(ContextCommandHandler)
        handler.manager.remove(context_name)
        echo_result('Context', 'red', 'remove', f'{context_name}')
    except AssertionError as e:
        echo_result('Context', 'yellow', 'aborted', str(e))
        exit(1)


@command(context_command_group)
def rename(old_name: str, new_name: str):
    """ Rename a context """
    try:
        handler: ContextCommandHandler = container.get(ContextCommandHandler)
        handler.manager.rename(old_name, new_name)
        echo_result('Context', 'green', 'rename', f'{old_name} → {new_name}')
    except AssertionError as e:
        echo_result('Context', 'yellow', 'aborted', str(e))
        exit(1)


@command(context_command_group, 'list', specs=[RESOURCE_OUTPUT_SPEC])
def list_context_names(output: Optional[str] = None):
    """ List all available context names """
    handler: ContextCommandHandler = container.get(ContextCommandHandler)
    show_iterator(
        output,
        handler.manager.list(),
        transform=lambda metadata: metadata.name,
        item_marker=lambda metadata: 'current' if metadata.selected else None
    )


@Service()
class ContextCommandHandler:
    _re_http_scheme = re.compile(r'^https?://')

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
        self._logger = get_logger(type(self).__name__, logging.DEBUG)
        self._context_manager: ContextManager = container.get(ContextManager)
        self._context_manager.events.on('context-sync', self.__handle_sync_event)
        self._context_manager.events.on('auth-begin', handle_auth_begin)
        self._context_manager.events.on('auth-end', handle_auth_end)
        self._context_manager.events.on('no-refresh-token', handle_no_refresh_token)
        self._context_manager.events.on('refresh-skipped', handle_refresh_skipped)

    @property
    def manager(self):
        return self._context_manager

    def use(self, registry_hostname_or_url: str, context_name: Optional[str] = None, no_auth: bool = False):
        echo_result('Context', 'blue', 'syncing', registry_hostname_or_url)
        self._context_manager.use(registry_hostname_or_url, context_name=context_name, no_auth=no_auth)
        echo_result('Context', 'green', 'use', registry_hostname_or_url)

    def __handle_sync_event(self, event: Event):
        action: str = event.details['action']
        endpoint: ServiceEndpoint = event.details['endpoint']

        echo_result(
            'Endpoint',
            self.__output_color_map[action],
            action,
            f'{endpoint.id} ({endpoint.type.group}:{endpoint.type.artifact}:{endpoint.type.version}) at {endpoint.url}',
            self.__emoji_map[action],
        )
