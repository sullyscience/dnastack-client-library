import click
from imagination import container
from typing import Optional

from dnastack.client.data_connect import DataConnectClient
from .helper import handle_query
from dnastack.cli.helpers.exporter import to_json, to_yaml
from dnastack.cli.helpers.client_factory import ConfigurationBasedClientFactory
from dnastack.cli.helpers.command.decorator import command
from dnastack.cli.helpers.command.spec import ArgumentSpec, DATA_OUTPUT_SPEC, RESOURCE_OUTPUT_SPEC
from ..helpers.command.group import AliasedGroup
from ..helpers.iterator_printer import show_iterator

DECIMAL_POINT_OUTPUT_SPEC = ArgumentSpec(
    name='decimal_as',
    arg_names=['--decimal-as'],
    as_option=True,
    help='The format of the decimal value',
    choices=["string", "float"],
)


def _get(context: Optional[str] = None, id: Optional[str] = None) -> DataConnectClient:
    factory: ConfigurationBasedClientFactory = container.get(ConfigurationBasedClientFactory)
    return factory.get(DataConnectClient, context_name=context, endpoint_id=id)


@click.group("data-connect", cls=AliasedGroup, aliases=["dataconnect", "dc"])
def data_connect_command_group():
    """ Interact with Data Connect Service """


@command(data_connect_command_group,
         'query',
         [
             DATA_OUTPUT_SPEC,
             DECIMAL_POINT_OUTPUT_SPEC,
         ])
def data_connect_query(context: Optional[str],
                       endpoint_id: Optional[str],
                       query: str,
                       output: Optional[str] = None,
                       decimal_as: str = 'string',
                       no_auth: bool = False):
    """ Perform a search query """
    return handle_query(_get(context=context, id=endpoint_id),
                        query,
                        decimal_as=decimal_as,
                        no_auth=no_auth,
                        output_format=output)


@click.group("tables")
def table_command_group():
    """ Table API commands """


@command(table_command_group, 'list', specs=[RESOURCE_OUTPUT_SPEC])
def list_tables(context: Optional[str],
                endpoint_id: Optional[str],
                no_auth: bool = False,
                output: Optional[str] = None):
    """ List all accessible tables """
    show_iterator(output, _get(context=context, id=endpoint_id).iterate_tables(no_auth=no_auth))


@command(table_command_group, 'get', specs=[RESOURCE_OUTPUT_SPEC])
def get_table_info(context: Optional[str],
                   endpoint_id: Optional[str],
                   table_name: str,
                   no_auth: bool = False,
                   output: Optional[str] = None):
    """ Get info from the given table """
    obj = _get(context=context, id=endpoint_id).table(table_name, no_auth=no_auth).info.dict()
    click.echo((to_json if output == 'json' else to_yaml)(obj))


# noinspection PyTypeChecker
data_connect_command_group.add_command(table_command_group)
