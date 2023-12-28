from typing import Optional

import click

from dnastack.cli.data_connect.commands import _get, DECIMAL_POINT_OUTPUT_SPEC
from dnastack.cli.helpers.command.decorator import command
from dnastack.cli.helpers.command.group import AliasedGroup
from dnastack.cli.helpers.command.spec import DATA_OUTPUT_SPEC
from dnastack.cli.helpers.iterator_printer import show_iterator


@click.group("data-connect", cls=AliasedGroup, aliases=["dc"])
def alpha_data_connect_command_group():
    """ Interact with Data Connect Service (testing) """


@command(alpha_data_connect_command_group,
         'table-data',
         specs=[
             DATA_OUTPUT_SPEC,
             DECIMAL_POINT_OUTPUT_SPEC,
         ])
def get_table_data(context: Optional[str],
                   endpoint_id: Optional[str],
                   table_name: str,
                   decimal_as: str = 'string',
                   no_auth: bool = False,
                   output: Optional[str] = None):
    """ Get data from the given table """
    table = _get(context=context, id=endpoint_id).table(table_name, no_auth=no_auth)
    show_iterator(output, table.data, decimal_as=decimal_as, sort_keys=False)
