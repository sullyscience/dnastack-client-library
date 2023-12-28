from threading import Lock

import click
import os
from imagination import container
from typing import List, Dict, Optional

from dnastack.cli.helpers.command.decorator import command
from dnastack.cli.helpers.command.spec import ArgumentSpec
from dnastack.cli.helpers.command.group import AliasedGroup
from dnastack.cli.helpers.printer import echo_result
from dnastack.client.drs import DownloadOkEvent, DownloadFailureEvent, DownloadProgressEvent, DrsClient
from dnastack.feature_flags import in_interactive_shell
from dnastack.cli.helpers.client_factory import ConfigurationBasedClientFactory


def _get(context: Optional[str], id: Optional[str] = None) -> DrsClient:
    factory: ConfigurationBasedClientFactory = container.get(ConfigurationBasedClientFactory)
    return factory.get(DrsClient, context_name=context, endpoint_id=id)


@click.group('files', cls=AliasedGroup, aliases=["drs"])
def drs_command_group():
    """ Interact with Data Repository Service """


@command(
    drs_command_group,
    specs=[
        ArgumentSpec(
            name='quiet',
            arg_names=['-q', '--quiet'],
            help='Download files quietly',
            required=False,
        ),
        ArgumentSpec(
            name='input_file',
            arg_names=['-i', '--input-file'],
            help='Input file',
            required=False,
            as_option=True,
        ),
        ArgumentSpec(
            name='output_dir',
            arg_names=['-o', '--output-dir'],
            help='Output directory',
            required=False,
            as_option=True,
        ),
        ArgumentSpec(
            name='id_or_urls',
            help='DRS IDs or URLs (drs://<host>/<id>)',
            required=False,
            nargs=-1,
        )
    ]
)
def download(context: Optional[str],
             endpoint_id: str,
             id_or_urls: List[str],
             output_dir: str = os.getcwd(),
             input_file: str = None,
             quiet: bool = False,
             no_auth: bool = False):
    """
    Download files with either DRS IDs or URLs, e.g., drs://<hostname>/<drs_id>.

    You can find out more about DRS URLs from the Data Repository Service Specification 1.1.0 at
    https://ga4gh.github.io/data-repository-service-schemas/preview/release/drs-1.1.0/docs/#_drs_uris.
    """
    output_lock = Lock()
    download_urls = []
    full_output = not quiet and in_interactive_shell

    if len(id_or_urls) > 0:
        download_urls = list(id_or_urls)
    elif input_file:
        with open(input_file, "r") as infile:
            download_urls = filter(None, infile.read().split("\n"))  # need to filter out invalid values
    else:
        if in_interactive_shell:
            click.echo("Enter one or more URLs. Press q to quit")

        while True:
            try:
                url = click.prompt("", prompt_suffix="", type=str)
                url = url.strip()
                if url[0] == "q" or len(url) == 0:
                    break
            except click.Abort:
                break

            download_urls.append(url)

    drs = _get(context, endpoint_id)

    def display_ok(event: DownloadOkEvent):
        with output_lock:
            if full_output:
                print()
            echo_result(None, 'green', 'complete', event.drs_url)
            if event.output_file_path:
                click.secho(f' → Saved as {event.output_file_path}', dim=True)

    def display_failure(event: DownloadFailureEvent):
        with output_lock:
            if not quiet:
                print()
            echo_result(None, 'red', 'failed', event.drs_url)
            if event.reason:
                click.secho(f' ● Reason: {event.reason}', dim=True)
            if event.error:
                click.secho(f' ● Error: {type(event.error).__name__}: {event.error}', dim=True)

    drs.events.on('download-ok', display_ok)
    drs.events.on('download-failure', display_failure)

    stats: Dict[str, DownloadProgressEvent] = dict()

    if not full_output:
        drs._download_files(id_or_urls=download_urls,
                            output_dir=output_dir,
                            no_auth=no_auth)
    else:
        with click.progressbar(label='Downloading...', color=True, length=1) as progress:
            def update_progress(event: DownloadProgressEvent):
                stats[event.drs_url] = event

                # Update the progress bar.
                position = 0
                total = 0

                for e in stats.values():
                    position += e.read_byte_count
                    total += e.total_byte_count

                progress.pos = position
                progress.length = total if total > 0 else 1

                progress.render_progress()

            drs.events.on('download-progress', update_progress)
            drs._download_files(id_or_urls=download_urls,
                                output_dir=output_dir,
                                no_auth=no_auth)
        print('DONE')
