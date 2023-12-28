from typing import Optional

import click
from imagination import container

from dnastack.alpha.client.collections.client import CollectionServiceClient
from dnastack.cli.collections import COLLECTION_ID_CLI_ARG_SPEC, _abort_with_collection_list, _filter_collection_fields, \
    _simplify_collection, _transform_to_public_collection
from dnastack.cli.helpers.client_factory import ConfigurationBasedClientFactory
from dnastack.cli.helpers.command.decorator import command
from dnastack.cli.helpers.command.spec import RESOURCE_OUTPUT_SPEC, ArgumentSpec
from dnastack.cli.helpers.exporter import normalize, to_yaml, to_json
from dnastack.cli.helpers.iterator_printer import OutputFormat, show_iterator
from dnastack.common.logger import get_logger
from dnastack.common.tracing import Span

_logger = get_logger('alpha/cli/collections')


def _get(context: Optional[str] = None, id: Optional[str] = None) -> CollectionServiceClient:
    factory: ConfigurationBasedClientFactory = container.get(ConfigurationBasedClientFactory)
    return factory.get(CollectionServiceClient, context_name=context, endpoint_id=id)


@click.group("collections")
def alpha_collection_command_group():
    """ Interact with Collection Service or Explorer Service.
    """


@command(alpha_collection_command_group,
         'list',
         specs=[
             COLLECTION_ID_CLI_ARG_SPEC,
             RESOURCE_OUTPUT_SPEC,
         ])
def list_collections(context: Optional[str],
                     endpoint_id: Optional[str],
                     collection: str,
                     output: Optional[str] = None):
    """ List the collections """
    span = Span(origin='alpha.cli.collections.list')
    show_iterator(output,
                  [
                      _filter_collection_fields(_simplify_collection(collection))
                      for collection in _get(context, endpoint_id).list_collections(trace=span)
                  ],
                  transform=_transform_to_public_collection)


@command(alpha_collection_command_group,
         specs=[
             COLLECTION_ID_CLI_ARG_SPEC,
             RESOURCE_OUTPUT_SPEC,
         ])
def get(context: Optional[str],
        endpoint_id: Optional[str],
        collection: str,
        output: Optional[str] = None):
    """ Get the collection """
    trace = Span(origin='alpha.cli.collections.get')

    client = _get(context, endpoint_id)
    if not collection:
        _abort_with_collection_list(client, collection, no_auth=False)

    result = client.get(collection, trace=trace)
    normalized_result = normalize(result)

    # NOTE: As returning the output is not critical, we will assume that if the output format is not recognized,
    #       the code will not raise an error on this and will use JSON as the default format.
    click.echo((to_yaml if output == OutputFormat.YAML else to_json)(normalized_result))


@command(alpha_collection_command_group,
         specs=[
             COLLECTION_ID_CLI_ARG_SPEC,
             ArgumentSpec(
                 name='name',
                 arg_names=['--name'],
                 as_option=True,
                 help='The new name of the collection',
                 required=False,
             ),
             ArgumentSpec(
                 name='slug_name',
                 arg_names=['--slug-name'],
                 as_option=True,
                 help='The new slug name of the collection',
                 required=False,
             ),
             ArgumentSpec(
                 name='description',
                 arg_names=['--description'],
                 as_option=True,
                 help='The new description of the collection',
                 required=False,
             ),
             RESOURCE_OUTPUT_SPEC,
         ])
def patch(context: Optional[str],
          endpoint_id: Optional[str],
          collection: str,
          name: Optional[str] = None,
          slug_name: Optional[str] = None,
          description: Optional[str] = None,
          output: Optional[str] = None):
    """ Patch the collection """
    trace = Span(origin='alpha.cli.collections.patch')

    client = _get(context, endpoint_id)
    if not collection:
        _abort_with_collection_list(client, collection, no_auth=False)

    result = client.patch(id=collection, trace=trace, name=name, slugName=slug_name, description=description)
    normalized_result = normalize(result)

    # NOTE: As returning the output is not critical, we will assume that if the output format is not recognized,
    #       the code will not raise an error on this and will use JSON as the default format.
    click.echo((to_yaml if output == OutputFormat.YAML else to_json)(normalized_result))
