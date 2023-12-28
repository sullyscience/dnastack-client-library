import click

from dnastack.cli.workbench.runs_commands import runs_command_group
from dnastack.cli.workbench.workflows_commands import workflows_command_group


@click.group('workbench')
def workbench_command_group():
    """ Interact with Workbench """


workbench_command_group.add_command(runs_command_group)
workbench_command_group.add_command(workflows_command_group)
