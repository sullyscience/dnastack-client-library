from typing import Optional

from click import Group, Command, Context, HelpFormatter


class AliasedGroup(Group):
    """
    A click Group wrapper, enabling command shortcuts/aliases for the group and its subgroups
    """

    def __init__(self, *args, **kwargs):
        self.aliases = kwargs.pop('aliases', [])
        super(AliasedGroup, self).__init__(*args, **kwargs)
        self.sub_commands = {}
        self.sub_aliases = {}

    def add_command(self, cmd: Command, name: Optional[str] = None) -> None:
        super().add_command(cmd, name)
        if hasattr(cmd, 'aliases'):
            self.sub_commands[cmd.name] = cmd.aliases
            for sub_alias in cmd.aliases:
                self.sub_aliases[sub_alias] = cmd.name

    def resolve_alias(self, cmd_name: str) -> str:
        if cmd_name in self.sub_aliases:
            return self.sub_aliases[cmd_name]
        return cmd_name

    def get_command(self, ctx: Context, cmd_name: str) -> Optional[Command]:
        cmd_name = self.resolve_alias(cmd_name)
        command = super(AliasedGroup, self).get_command(ctx, cmd_name)
        if command:
            return command

    def format_commands(self, ctx: Context, formatter: HelpFormatter) -> None:
        rows = []

        sub_commands = self.list_commands(ctx)

        max_len = max(len(cmd) for cmd in sub_commands)
        limit = formatter.width - 6 - max_len

        for sub_command in sub_commands:
            cmd = self.get_command(ctx, sub_command)
            if cmd is None:
                continue
            if hasattr(cmd, 'hidden') and cmd.hidden:
                continue
            if sub_command in self.sub_commands:
                aliases = self.sub_commands[sub_command]
                if len(aliases) > 0:
                    aliases = ",".join(sorted(aliases))
                    sub_command = "{0} ({1})".format(sub_command, aliases)
            cmd_help = cmd.get_short_help_str(limit)
            rows.append((sub_command, cmd_help))

        if rows:
            with formatter.section('Commands'):
                formatter.write_dl(rows)