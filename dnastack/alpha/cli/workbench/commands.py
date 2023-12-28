import click

from dnastack.alpha.cli.workbench.engines_commands import alpha_engines_command_group
from dnastack.alpha.cli.workbench.workflows_commands import alpha_workflows_command_group


@click.group('workbench')
def alpha_workbench_command_group():
    """ Interact with Workbench """


alpha_workbench_command_group.add_command(alpha_workflows_command_group)
alpha_workbench_command_group.add_command(alpha_engines_command_group)
