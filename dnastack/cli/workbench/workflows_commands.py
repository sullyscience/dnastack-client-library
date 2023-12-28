from typing import Optional

import click
from click import style

from dnastack.cli.workbench.utils import get_workflow_client
from dnastack.client.workbench.workflow.models import WorkflowCreate, WorkflowVersionCreate, WorkflowSource, \
    WorkflowListOptions, WorkflowVersionListOptions
from dnastack.http.session import JsonPatch
from dnastack.client.workbench.workflow.utils import WorkflowSourceLoader
from dnastack.cli.helpers.command.decorator import command
from dnastack.cli.helpers.command.spec import ArgumentSpec
from dnastack.cli.helpers.exporter import to_json, normalize
from dnastack.cli.helpers.iterator_printer import show_iterator, OutputFormat
from dnastack.common.json_argument_parser import *


@click.group('versions')
def workflow_versions_command_group():
    """ Create and interact with workflow versions """


@click.group('workflows')
def workflows_command_group():
    """ Create and interact with  workflows"""


def _get_author_patch(authors: str) -> Union[JsonPatch, None]:
    if authors == "":
        return JsonPatch(path="/authors", op="remove")
    elif authors:
        return JsonPatch(path="/authors", op="replace", value=authors.split(","))
    return None


def _get_description_patch(description: Optional[FileOrValue]) -> Union[JsonPatch, None]:
    if not description:
        return None
    if description.raw_value == "":
        return JsonPatch(path="/description", op="remove")
    elif description:
        return JsonPatch(path="/description", op="replace", value=description.value())
    return None


def _get_replace_patch(path: str, value: str) -> Union[JsonPatch, None]:
    if value:
        return JsonPatch(path=path, op="replace", value=value)
    return None


@command(workflows_command_group,
         'list',
         specs=[
             ArgumentSpec(
                 name='namespace',
                 arg_names=['--namespace', '-n'],
                 help='An optional flag to define the namespace to connect to. By default, the namespace will be '
                      'extracted from the users credentials.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='max_results',
                 arg_names=['--max-results'],
                 help='Limit the total number of results.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='page',
                 arg_names=['--page'],
                 help='Set the page number. '
                      'This allows for jumping into an arbitrary page of results. Zero-based.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='page_size',
                 arg_names=['--page-size'],
                 help='Set the page size returned by the server.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='order',
                 arg_names=['--order'],
                 help='Define the ordering of the results. '
                      'The value should return to the attribute name to order the results by. '
                      'By default, results are returned in descending order. '
                      'To change the direction of ordering include the "ASC" or "DESC" string after the column. '
                      'e.g.: --O "end_time", --O "end_time ASC"',

                 as_option=True
             ),
             ArgumentSpec(
                 name='source',
                 arg_names=['--source'],
                 help='Filter the results to only include workflows from the defined source. '
                      'Note: The CUSTOM workflow source has been renamed to Private',
                 as_option=True,
                 required=False,
                 default=None,
                 type=WorkflowSource,
                 choices=[e.value for e in WorkflowSource]

             ),
             ArgumentSpec(
                 name='search',
                 arg_names=['--search'],
                 help='Perform a full text search across various fields using the search value',
                 as_option=True
             ),
             ArgumentSpec(
                 name='include_deleted',
                 arg_names=['--include-deleted'],
                 help='Include deleted workflows in the list',
                 as_option=True

             ),
         ]
         )
def list_workflows(context: Optional[str],
                   endpoint_id: Optional[str],
                   namespace: Optional[str],
                   max_results: Optional[int],
                   page: Optional[int],
                   page_size: Optional[int],
                   order: Optional[str],
                   search: Optional[str],
                   source: Optional[WorkflowSource],
                   include_deleted: Optional[bool] = False):
    """
    List workflows

    docs: https://docs.omics.ai/docs/workflows-list
    """
    order_direction = None
    if order:
        order_and_direction = order.split()
        if len(order_and_direction) > 1:
            order = order_and_direction[0]
            order_direction = order_and_direction[1]

    ## Migration
    if source and source == WorkflowSource.custom:
        source = WorkflowSource.private

    workflows_client = get_workflow_client(context, endpoint_id, namespace)
    list_options = WorkflowListOptions(
        page=page,
        page_size=page_size,
        order=order,
        direction=order_direction,
        source=source,
        search=search,
        deleted=include_deleted
    )
    show_iterator(output_format=OutputFormat.JSON,
                  iterator=workflows_client.list_workflows(list_options=list_options, max_results=max_results))


@command(workflows_command_group,
         'describe',
         specs=[
             ArgumentSpec(
                 name='namespace',
                 arg_names=['--namespace', '-n'],
                 help='An optional flag to define the namespace to connect to. By default, the namespace will be '
                      'extracted from the users credentials.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='include_deleted',
                 arg_names=['--include-deleted'],
                 help='An optional flag to include deleted workflows in the list',
                 as_option=True

             ),
         ]
         )
def describe_workflows(context: Optional[str],
                       endpoint_id: Optional[str],
                       namespace: Optional[str],
                       workflows: List[str],
                       include_deleted: Optional[bool] = False):
    """
    Describe one or more workflows

    docs: https://docs.omics.ai/docs/workflows-describe
    """
    workflows_client = get_workflow_client(context, endpoint_id, namespace)

    if not workflows:
        click.echo(style("You must specify at least one workflow ID", fg='red'), err=True, color=True)
        exit(1)

    described_workflows = [workflows_client.get_workflow(workflow_id, include_deleted=include_deleted) for workflow_id
                           in workflows]
    click.echo(to_json(normalize(described_workflows)))


@command(workflows_command_group,
         'create',
         specs=[
             ArgumentSpec(
                 name='namespace',
                 arg_names=['--namespace', '-n'],
                 help='An optional flag to define the namespace to connect to. By default, the namespace will be '
                      'extracted from the users credentials.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='name',
                 arg_names=['--name'],
                 help='An optional flag to show set a workflow name. If omitted, the name within the workflow will be used',
                 as_option=True

             ),
             ArgumentSpec(
                 name='version_name',
                 arg_names=['--version-name'],
                 help='An optional flag to show set the version name. If omitted, v1_0_0 will be used',
                 as_option=True

             ),
             ArgumentSpec(
                 name='description',
                 arg_names=['--description'],
                 help='An optional flag to set a description for the workflow'
                      ' You can specify a file by prepending "@" to a path: @<path>',
                 as_option=True,
                 required=False,
                 default=None

             )
         ]
         )
def create_workflow(context: Optional[str],
                    endpoint_id: Optional[str],
                    namespace: Optional[str],
                    name: Optional[str],
                    version_name: Optional[str],
                    description: FileOrValue,
                    source_files: List[str]):
    """
    Create a new workflow

    The first file ending in ".wdl" will be treated as the entrypoint for the entire workflow
    becoming the "PRIMARY_DESCRIPTOR". If there are any local imports in a WDL file they will be dynamically resolved
    relative to the entrypoint.

    Files that are not WDL files may be included in the request and will have their file type set as follows:

     - files ending in ".json" will be set to type: "TEST_FILE"

     - files ending in any other extension will be set to type "OTHER"

    docs: https://docs.omics.ai/docs/workflows-create
    """

    workflows_client = get_workflow_client(context, endpoint_id, namespace)
    workflow_source = WorkflowSourceLoader(source_files)

    create_request = WorkflowCreate(
        name=name,
        versionName=version_name,
        description=description.value() if description else None,
        files=workflow_source.loaded_files
    )

    result = workflows_client.create_workflow(workflow_create_request=create_request)
    click.echo(to_json(normalize(result)))


@command(workflows_command_group,
         "delete",
         specs=[
             ArgumentSpec(
                 name='namespace',
                 arg_names=['--namespace', '-n'],
                 help='An optional flag to define the namespace to connect to. By default, the namespace will be '
                      'extracted from the users credentials.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='force',
                 arg_names=['--force'],
                 help='Force the deletion without prompting for confirmation.',
                 as_option=True,
                 default=False
             ),
         ]
         )
def delete_workflow(context: Optional[str],
                    endpoint_id: Optional[str],
                    namespace: Optional[str],
                    workflow_id: str,
                    force: Optional[bool] = False):
    """
    Delete an existing workflow

    docs: https://docs.omics.ai/docs/workflows-delete
    """
    workflows_client = get_workflow_client(context, endpoint_id, namespace)
    workflow = workflows_client.get_workflow(workflow_id)
    if not force and not click.confirm(
            f'Do you want to delete "{workflow.name}"?'):
        return

    workflows_client.delete_workflow(workflow.internalId, workflow.etag)
    click.echo("Deleted...")


@command(workflows_command_group,
         "update",
         specs=[
             ArgumentSpec(
                 name='namespace',
                 arg_names=['--namespace', '-n'],
                 help='An optional flag to define the namespace to connect to. By default, the namespace will be '
                      'extracted from the users credentials.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='name',
                 arg_names=['--name'],
                 help='The new name of the workflow',
                 as_option=True
             ),
             ArgumentSpec(
                 name='description',
                 arg_names=['--description'],
                 help='The new description of the workflow in markdown format.'
                      ' You can specify a file by prepending "@" to a path: @<path>. To'
                      ' unset the description the value should be ""',
                 as_option=True
             ),
             ArgumentSpec(
                 name='authors',
                 arg_names=['--authors'],
                 help='List of authors to update. This value can be a comma separated list, a file or JSON literal',
                 as_option=True,
                 required=False,
                 default=None
             ),
         ]
         )
def update_workflow(context: Optional[str],
                    endpoint_id: Optional[str],
                    namespace: Optional[str],
                    workflow_id: str,
                    name: Optional[str],
                    description: FileOrValue,
                    authors: Optional[str]):
    """
    Update an existing workflow

    docs: https://docs.omics.ai/docs/workflows-update
    """
    workflows_client = get_workflow_client(context, endpoint_id, namespace)
    workflow = workflows_client.get_workflow(workflow_id)

    patch_list = [
        _get_replace_patch("/name", name),
        _get_description_patch(description),
        _get_author_patch(authors)
    ]
    patch_list = [patch for patch in patch_list if patch]

    if patch_list:
        workflow = workflows_client.update_workflow(workflow_id, workflow.etag, patch_list)
        click.echo(to_json(normalize(workflow)))
    else:
        raise ValueError("Must specify at least one attribute to update")


@command(workflow_versions_command_group,
         'list',
         specs=[
             ArgumentSpec(
                 name='namespace',
                 arg_names=['--namespace', '-n'],
                 help='An optional flag to define the namespace to connect to. By default, the namespace will be '
                      'extracted from the users credentials.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='workflow',
                 arg_names=['--workflow', ],
                 help='The workflow id to add the version to.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='max_results',
                 arg_names=['--max-results'],
                 help='Limit the total number of results.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='include_deleted',
                 arg_names=['--include-deleted'],
                 help='An optional flag to include deleted workflows in the list',
                 as_option=True

             ),
         ]
         )
def list_versions(context: Optional[str],
                  endpoint_id: Optional[str],
                  namespace: Optional[str],
                  workflow: str,
                  max_results: Optional[int],
                  include_deleted: Optional[bool] = False
                  ):
    """
    List the available versions for the given workflow

    docs: https://docs.omics.ai/docs/workflows-versions-list
    """
    workflows_client = get_workflow_client(context, endpoint_id, namespace)
    list_options = WorkflowVersionListOptions(
        deleted=include_deleted
    )
    show_iterator(output_format=OutputFormat.JSON,
                  iterator=workflows_client.list_workflow_versions(workflow_id=workflow,
                                                                   list_options=list_options,
                                                                   max_results=max_results))


@command(workflow_versions_command_group,
         'describe',
         specs=[
             ArgumentSpec(
                 name='namespace',
                 arg_names=['--namespace', '-n'],
                 help='An optional flag to define the namespace to connect to. By default, the namespace will be '
                      'extracted from the users credentials.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='workflow',
                 arg_names=['--workflow', ],
                 help='The workflow id to add the version to.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='include_deleted',
                 arg_names=['--include-deleted'],
                 help='An optional flag to include deleted workflows in the list',
                 as_option=True

             ),
         ]
         )
def describe_version(context: Optional[str],
                     endpoint_id: Optional[str],
                     namespace: Optional[str],
                     workflow: str,
                     versions: List[str],
                     include_deleted: Optional[bool] = False
                     ):
    """
    Describe one or more workflow versions for the given workflow

    docs: https://docs.omics.ai/docs/workflows-versions-describe
    """
    workflows_client = get_workflow_client(context, endpoint_id, namespace)
    click.echo(to_json(normalize(
        [workflows_client.get_workflow_version(workflow_id=workflow, version_id=version_id,
                                               include_deleted=include_deleted) for version_id in versions]
    )))


@command(workflow_versions_command_group,
         "delete",
         specs=[
             ArgumentSpec(
                 name='namespace',
                 arg_names=['--namespace', '-n'],
                 help='An optional flag to define the namespace to connect to. By default, the namespace will be '
                      'extracted from the users credentials.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='force',
                 arg_names=['--force'],
                 help='Force the deletion without prompting for confirmation.',
                 as_option=True,
                 default=False
             ),
             ArgumentSpec(
                 name='workflow_id',
                 arg_names=['--workflow'],
                 help='The id of the workflow',
                 as_option=True
             ),
         ]
         )
def delete_workflow_version(context: Optional[str],
                            endpoint_id: Optional[str],
                            namespace: Optional[str],
                            workflow_id: str,
                            version_id: str,
                            force: Optional[bool] = False):
    """
    Delete an existing workflow version

    docs: https://docs.omics.ai/docs/workflows-versions-delete
    """
    workflows_client = get_workflow_client(context, endpoint_id, namespace)
    workflow = workflows_client.get_workflow(workflow_id)
    version = workflows_client.get_workflow_version(workflow_id, version_id)
    if not force and not click.confirm(
            f'Do you want to delete "{version.versionName}" from workflow "{workflow.name}"?'):
        return

    workflows_client.delete_workflow_version(workflow_id, version_id, version.etag)
    click.echo("Deleted...")


@command(workflow_versions_command_group,
         'create',
         specs=[
             ArgumentSpec(
                 name='namespace',
                 arg_names=['--namespace', '-n'],
                 help='An optional flag to define the namespace to connect to. By default, the namespace will be '
                      'extracted from the users credentials.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='workflow',
                 arg_names=['--workflow', ],
                 help='The workflow id to add the version to.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='name',
                 arg_names=['--name'],
                 help='The version name to create',
                 as_option=True
             ),
             ArgumentSpec(
                 name='description',
                 arg_names=['--description'],
                 help='An optional description for the workflow version in markdown format.'
                      ' You can specify a file by prepending "@" to a path: @<path>',
                 as_option=True,
                 required=False,
                 default=None
             )
         ]
         )
def add_version(context: Optional[str],
                endpoint_id: Optional[str],
                namespace: Optional[str],
                workflow: str,
                name: str,
                description: FileOrValue,
                source_files: List[str], ):
    """
    Add a new version to an existing workflow

    The first file ending in ".wdl" will be treated as the entrypoint for the entire workflow
    becoming the "PRIMARY_DECSRIPTOR". If there are any local imports in a WDL file they will be dynamically resolved
    relative to the entrypoint.

    Files that are not WDL files may be included in the request and will have their file type set as follows:

     - files ending in ".json" will be set to type: "TEST_FILE"

     - files ending in any other extension will be set to type "OTHER"


    docs: https://docs.omics.ai/docs/workflows-versions-create
    """
    workflows_client = get_workflow_client(context, endpoint_id, namespace)
    workflow_source = WorkflowSourceLoader(source_files)

    create_request = WorkflowVersionCreate(
        versionName=name,
        description=description.value() if description else None,
        files=workflow_source.loaded_files
    )

    result = workflows_client.create_version(workflow_id=workflow, workflow_version_create_request=create_request)
    click.echo(to_json(normalize(result)))


@command(workflow_versions_command_group,
         "update",
         specs=[
             ArgumentSpec(
                 name='namespace',
                 arg_names=['--namespace', '-n'],
                 help='An optional flag to define the namespace to connect to. By default, the namespace will be '
                      'extracted from the users credentials.',
                 as_option=True
             ),
             ArgumentSpec(
                 name='version_name',
                 arg_names=['--name'],
                 help='The new name of the workflow version',
                 as_option=True
             ),
             ArgumentSpec(
                 name='description',
                 arg_names=['--description'],
                 help='The new description of the workflow version in markdown format.'
                      ' You can specify a file by prepending "@" to a path: @<path>. To'
                      ' unset the description the value should be ""',
                 as_option=True,
                 required=False,
                 default=None
             ),
             ArgumentSpec(
                 name='authors',
                 arg_names=['--authors'],
                 help='List of authors to update. This value can be a comma separated list',
                 as_option=True
             ),
             ArgumentSpec(
                 name='workflow_id',
                 arg_names=['--workflow'],
                 help='The id of the workflow',
                 as_option=True
             ),
         ]
         )
def update_workflow_version(context: Optional[str],
                            endpoint_id: Optional[str],
                            namespace: Optional[str],
                            workflow_id: str,
                            version_id: str,
                            version_name: Optional[str],
                            description: FileOrValue,
                            authors: Optional[str]):
    """
    Update an existing workflow version

    docs: https://docs.omics.ai/docs/workflows-versions-update
    """
    workflows_client = get_workflow_client(context, endpoint_id, namespace)
    workflow_version = workflows_client.get_workflow_version(workflow_id, version_id)

    patch_list = [
        _get_replace_patch("/versionName", version_name),
        _get_description_patch(description),
        _get_author_patch(authors)
    ]
    patch_list = [patch for patch in patch_list if patch]

    if patch_list:
        workflow_version = workflows_client.update_workflow_version(workflow_id, version_id, workflow_version.etag,
                                                                    patch_list)
        click.echo(to_json(normalize(workflow_version)))
    else:
        raise ValueError("Must specify at least one attribute to update")


workflows_command_group.add_command(workflow_versions_command_group)
