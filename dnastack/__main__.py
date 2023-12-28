import sys
from typing import Optional

import click

from dnastack.cli.auth.command import auth
from dnastack.cli.config.commands import config_command_group
from dnastack.cli.collections import collection_command_group
from dnastack.cli.config.context import context_command_group, ContextCommandHandler
from dnastack.cli.data_connect.commands import data_connect_command_group
from dnastack.cli.drs import drs_command_group
from dnastack.alpha.cli.commands import alpha_command_group
from dnastack.cli.helpers.command.decorator import command
from dnastack.cli.helpers.command.spec import ArgumentSpec
from dnastack.cli.helpers.command.group import AliasedGroup
from dnastack.cli.workbench.commands import workbench_command_group
from dnastack.common.logger import get_logger
from dnastack.constants import __version__

APP_NAME = sys.argv[0]

__library_version = __version__
__python_version = str(sys.version).replace("\n", " ")
__app_signature = f'{APP_NAME} {__library_version} with Python {__python_version}'

_context_command_handler = ContextCommandHandler()


@click.group(APP_NAME, cls=AliasedGroup)
@click.version_option(__version__, message="%(version)s")
def dnastack():
    """
    DNAstack Client CLI

    https://dnastack.com
    """
    get_logger(APP_NAME).debug(__app_signature)


@command(dnastack)
def version():
    """ Show the version of CLI/library """
    click.echo(__app_signature)


@command(dnastack,
         specs=[
             ArgumentSpec(
                 name='context_name',
                 arg_names=['--name'],
                 as_option=True,
                 help='Context name -- default to hostname'
             )
         ])
def use(registry_hostname_or_url: str, context_name: Optional[str] = None, no_auth: bool = False):
    """
    Import a configuration from host's service registry (if available) or the corresponding public configuration from
    cloud storage. If "--no-auth" is not defined, it will automatically initiate all authentication.

    This will also switch the default context to the given hostname.

    This is a shortcut to dnastack config contexts use".
    """
    _context_command_handler.use(registry_hostname_or_url, context_name=context_name, no_auth=no_auth)


# noinspection PyTypeChecker
dnastack.add_command(data_connect_command_group)
# noinspection PyTypeChecker
dnastack.add_command(config_command_group)
# noinspection PyTypeChecker
dnastack.add_command(drs_command_group)
# noinspection PyTypeChecker
dnastack.add_command(auth)
# noinspection PyTypeChecker
dnastack.add_command(collection_command_group)
# noinspection PyTypeChecker
dnastack.add_command(context_command_group)
# noinspection PyTypeChecker
dnastack.add_command(alpha_command_group)
# noinspection PyTypeChecker
dnastack.add_command(workbench_command_group)

if __name__ == "__main__":
    dnastack.main(prog_name=APP_NAME)
