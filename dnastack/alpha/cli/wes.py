import click
import json
import re
import yaml
from imagination import container
from typing import Optional, List

from dnastack.alpha.client.wes.client import WesClient, RunRequest
from dnastack.cli.helpers.client_factory import ConfigurationBasedClientFactory
from dnastack.cli.helpers.command.decorator import command
from dnastack.cli.helpers.command.spec import ArgumentSpec, RESOURCE_OUTPUT_SPEC
from dnastack.cli.helpers.exporter import normalize, to_json, to_yaml
from dnastack.cli.helpers.iterator_printer import show_iterator, OutputFormat
from dnastack.constants import __version__
from dnastack.feature_flags import in_interactive_shell


def _get(context_name: Optional[str] = None,
         endpoint_id: Optional[str] = None) -> WesClient:
    factory: ConfigurationBasedClientFactory = container.get(ConfigurationBasedClientFactory)
    return factory.get(WesClient, endpoint_id=endpoint_id, context_name=context_name)


@click.group('wes')
def alpha_wes_command_group():
    """ Interact with Workflow Execution Service """


@command(alpha_wes_command_group,
         specs=[
             ArgumentSpec(
                 name='workflow_type',
                 help='Workflow type',
                 as_option=True,
             ),
             ArgumentSpec(
                 name='workflow_type_version',
                 help='Workflow type version',
                 as_option=True,
             ),
             ArgumentSpec(
                 name='manifest_file_path',
                 arg_names=['--manifest-file', '-f'],
                 help='The file path of the run manifest file',
                 as_option=True,
             ),
             ArgumentSpec(
                 name='workflow_url',
                 arg_names=['--workflow-url', '-u'],
                 help='The file path or URL to the workflow file (*.wdl)',
                 as_option=True,
             ),
             ArgumentSpec(
                 name='params',
             ),
             ArgumentSpec(
                 name='attachments',
                 arg_names=['--attach', '-a'],
                 help='Attachment for this workflow run',
                 as_option=True,
             ),
             ArgumentSpec(
                 name='tags',
                 arg_names=['--tag', '-t'],
                 help='Tag for this run in the key-value pattern, e.g., <key>=<value>',
                 as_option=True,
             ),
         ])
def submit(context: Optional[str],
           endpoint_id: Optional[str],
           manifest_file_path: Optional[str],
           workflow_url: Optional[str],
           params: List[str],
           attachments: List[str],
           tags: List[str],
           workflow_type: str = 'WDL',
           workflow_type_version: str = '1.0',
           dry_run: bool = False):
    """
    Submit a run request.

    Please note that the parameters (PARAMS) must be in these formats:

     - <key>=<string_value> to assign "key" with the value ("string_value") as string

     - <key>:=<json_string> to assign "key" with the value parsed from "json_string"

     - <key>:=@<json_file_path> to assign "key" with the data read from "json_file_path"

    [Examples]

        dnastack alpha wes submit -u sample.wdl -t t1=alpha -t t2=beta -a sample.wdl -a sample/foo.txt k1=abc k2:=123 k3:=true k4:='{"a": "1", "b": 2}'

        dnastack alpha wes submit -u https://faux-pub.dnastack.com/sample.wdl -t t1=alpha -t t2=beta -a sample.wdl -a sample/foo.txt k1=abc k2:=123 k3:=true k4:='{"a": "1", "b": 2}'

        dnastack alpha wes submit -f sample_run.json

        dnastack alpha wes submit -f sample_run.yaml

        dnastack alpha wes submit --endpoint-id testing_wes -u hello_world.wdl -a samples/workflows/no_input/hello_world.wdl
    """
    actual_params = dict()

    re_parameter = re.compile(r'^(?P<key>[a-zA-Z0-9_\.]+)(?P<op>:?=@?)(?P<value>.+)$')

    # Parse the workflow parameters.
    param_counter = 0
    for param in params:
        matches = re_parameter.search(param)
        if matches:
            data = matches.groupdict()
            value = data['value']
            if data['op'] == ':=':
                value = json.loads(value)
            elif data['op'] == ':=@':
                with open(value) as fp:
                    value = json.load(fp)

            actual_params[data['key']] = value
        else:
            raise ValueError(f'Param #{param_counter + 1} ({param}) is invalid.')

        param_counter += 1

    actual_tags = dict(agent=f'dnastack-client-library/{__version__}')
    if tags:
        tag_counter = 0
        for tag in tags:
            if '=' not in tag:
                raise ValueError(f'Tag #{tag_counter + 1} ({tag}) is invalid. The tag format is "<key>=<value>".')
            k, v = tag.split('=', 1)
            actual_tags[k] = v
            tag_counter += 1

    if manifest_file_path is None:
        run_request = RunRequest(
            workflow_url=workflow_url,
            workflow_params=actual_params,
            workflow_type=workflow_type,
            workflow_type_version=workflow_type_version,
            tags=actual_tags or None,
            attachments=attachments,
        )
    else:
        try:
            with open(manifest_file_path, 'r') as f:
                content = f.read()
        except FileNotFoundError:
            raise RuntimeError(f'The given manifest file (at {manifest_file_path}) does not exist.')

        if re.search(r'\.ya?ml$', manifest_file_path, re.IGNORECASE):
            raw_run_request = yaml.load(content, Loader=yaml.SafeLoader)
        elif re.search(r'\.json$', manifest_file_path, re.IGNORECASE):
            raw_run_request = json.loads(content)
        else:
            raise RuntimeError('Unsupported manifest file type. Currently only support JSON and YAML.')

        run_request = RunRequest(**raw_run_request)

        # Any given CLI arguments are treated as overrides.
        if workflow_url:
            run_request.workflow_url = workflow_url
        if actual_params:
            run_request.workflow_params.update(actual_params)
        if workflow_type:
            run_request.workflow_type = workflow_type
        if workflow_type_version:
            run_request.workflow_type_version = workflow_type_version
        if actual_tags:
            run_request.tags = actual_tags
        if attachments:
            run_request.attachments.extend(attachments)

    if dry_run:
        click.secho('WARNING: You are running in the dry-run mode and '
                    'this run request will not be submitted to the service endpoint.',
                    fg='yellow',
                    err=True)
        click.secho(run_request.json(indent=2), dim=True)
    else:
        _execute(context=context,
                 endpoint_id=endpoint_id,
                 run_request=run_request)


def _execute(run_request: RunRequest,
             context: Optional[str] = None,
             endpoint_id: Optional[str] = None):
    client = _get(context_name=context, endpoint_id=endpoint_id)
    run_id = client.submit(run_request)

    click.secho(f'Successfully submitted workflow run {run_id}', fg='green')

    _show_next_step(
        'Now, you can get the job details with this command',
        'get',
        context,
        endpoint_id,
        run_id
    )


@command(alpha_wes_command_group,
         'list',
         specs=[
             RESOURCE_OUTPUT_SPEC,
         ])
def list_runs(context: Optional[str],
              endpoint_id: Optional[str],
              output: Optional[str],
              limit: int = 10):
    """ List the most recent runs """
    client = _get(context_name=context, endpoint_id=endpoint_id)
    show_iterator(output_format=output, iterator=client.get_runs(), limit=limit)


@command(alpha_wes_command_group,
         specs=[
             RESOURCE_OUTPUT_SPEC,
         ])
def get(context: Optional[str],
        endpoint_id: Optional[str],
        output: Optional[str],
        run_id: str,
        verbose: bool = False):
    """ Get the run information (request, state, log URLs, etc.) """
    client = _get(context_name=context, endpoint_id=endpoint_id)
    run = client.run(run_id)
    info = run.info()

    # Get the output formatter.
    if output == OutputFormat.JSON:
        formatter = to_json
    elif output == OutputFormat.YAML:
        formatter = to_yaml
    else:
        raise NotImplementedError(output)

    # Output the data
    click.echo(formatter(normalize(
        {'run_id': info.run_id, 'state': info.state, 'outputs': info.outputs}
        if not verbose
        else info
    )))

    # Show suggestions for next steps.
    basic_arguments = [run_id]

    if endpoint_id:
        basic_arguments.insert(0, f'--endpoint-id {endpoint_id}')

    if context:
        basic_arguments.insert(0, f'--context {context}')

    if not verbose:
        _show_next_step(
            'Run this command to get the full details',
            'get',
            context,
            endpoint_id,
            run_id,
            ['--verbose'],
        )

    _show_next_step(
        'Run this command to get logs',
        'logs',
        context,
        endpoint_id,
        run_id,
    )


@command(alpha_wes_command_group)
def logs(context: Optional[str],
         endpoint_id: Optional[str],
         run_id: str,
         exclude_stderr: bool = False,
         verbose: bool = False):
    """ Show logs """
    client = _get(context_name=context, endpoint_id=endpoint_id)
    run = client.run(run_id)

    if verbose:
        click.secho(f'Status: {run.status}', err=True, fg='blue')

    for output in run.get_logs(include_stderr=not exclude_stderr):
        log_name = output.origin.name

        if output.is_empty():
            if verbose:
                click.secho(f'{log_name}: No output', err=True, fg='magenta')
            else:
                pass
        else:
            stdout = (output.stdout or str())
            stderr = (output.stderr or str())

            if verbose:
                click.secho(f'{log_name}: ({len(stdout) + len(stderr)} bytes in total)', err=True, fg='blue')

            if stdout:
                for line in stdout.split('\n'):
                    click.secho(f'{log_name}: ', dim=True, nl=False)
                    click.secho(line)

            if stderr:
                for line in stderr.split('\n'):
                    click.secho(f'{log_name}: ', dim=True, nl=False)
                    click.secho(line, fg='red')


def _show_next_step(suggestion: str,
                    command: str,
                    context: Optional[str],
                    endpoint_id: Optional[str],
                    run_id: str,
                    arguments: Optional[List[str]] = None):
    if not in_interactive_shell:
        return

    basic_arguments = [run_id]

    if endpoint_id:
        basic_arguments.insert(0, f'--endpoint-id {endpoint_id}')

    if context:
        basic_arguments.insert(0, f'--context {context}')

    if arguments:
        basic_arguments.extend(arguments)

    click.secho(f'| {suggestion}:\n|\n'
                f'|   dnastack alpha wes {command} {" ".join(basic_arguments)}\n|',
                dim=True,
                err=True)
