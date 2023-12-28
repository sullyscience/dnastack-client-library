import click
import json
from imagination import container

from dnastack.cli.config.context import context_command_group
from dnastack.cli.config.endpoints import endpoint_command_group
from dnastack.cli.config.service_registry import registry_command_group
from dnastack.cli.helpers.command.decorator import command
from dnastack.cli.helpers.command.group import AliasedGroup
from dnastack.configuration.manager import ConfigurationManager
from dnastack.configuration.models import Configuration


@click.group("config", cls=AliasedGroup)
def config_command_group():
    """ Manage global configuration """


@command(config_command_group, "schema")
def config_schema():
    """Show the schema of the configuration file"""
    click.echo(json.dumps(Configuration.schema(), indent=2, sort_keys=True))


@command(config_command_group)
def reset():
    """Reset the configuration file"""
    manager: ConfigurationManager = container.get(ConfigurationManager)
    manager.hard_reset()


# noinspection PyTypeChecker
config_command_group.add_command(registry_command_group)

# noinspection PyTypeChecker
config_command_group.add_command(endpoint_command_group)

# noinspection PyTypeChecker
config_command_group.add_command(context_command_group)
