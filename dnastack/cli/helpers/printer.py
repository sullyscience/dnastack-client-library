from shutil import get_terminal_size

import click
import json
from contextlib import contextmanager
from typing import Any, List, Optional, Dict


# ####################
# # Click Extensions #
# ####################
# class MutuallyExclusiveOption(Option):
#     """
#     A click Option wrapper for sets of options where one but not both must be specified
#     """
#
#     def __init__(self, *args, **kwargs):
#         self.mutually_exclusive = set(kwargs.pop("mutually_exclusive", []))
#         original_help = kwargs.get("help", "")
#         if self.mutually_exclusive:
#             additional_help_text = "This is mutually exclusive with " \
#                                    + " and ".join(sorted(self.mutually_exclusive)) + "."
#             kwargs[
#                 "help"] = f"{original_help}. Note that {additional_help_text}" if original_help else additional_help_text
#         super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)
#
#     def handle_parse_result(self, ctx: click.Context, opts: Mapping[str, Any], args: List[str]) -> Tuple[
#         Any, List[str]]:
#         if self.mutually_exclusive.intersection(opts) and self.name in opts:
#             raise UsageError(
#                 "Illegal usage: `{}` is mutually exclusive with "
#                 "arguments `{}`.".format(self.name, ", ".join(self.mutually_exclusive))
#             )
#
#         return super(MutuallyExclusiveOption, self).handle_parse_result(ctx, opts, args)


#################
# Output Helper #
#################
def echo_header(title: str, bg: str = 'blue', fg: str = 'white', err: bool = False, bold: bool = True, top_margin: int = 1, bottom_margin: int = 1):
    term_dimension = get_terminal_size((80, 24))
    term_width = term_dimension.columns

    text_length = min(term_width, 116)

    lines = truncate_text(title, text_length)
    max_line_length = max([len(line) for line in lines])

    column_size = max_line_length + 4
    vertical_padding = ' ' * column_size

    for __ in range(top_margin):
        print()

    click.secho(vertical_padding, bold=bold, bg=bg, fg=fg, err=err)
    for line in lines:
        click.secho(f'  {line}{" " * (max_line_length - len(line))}  ', bold=bold, bg=bg, fg=fg, err=err)
    click.secho(vertical_padding, bold=bold, bg=bg, fg=fg, err=err)

    for __ in range(bottom_margin):
        print()


def truncate_text(content: str, text_length: int) -> List[str]:
    blocks = content.split('\n')

    lines = []

    for block in blocks:
        lines.extend(truncate_paragraph(block, text_length))

    return lines


def truncate_paragraph(content: str, text_length: int) -> List[str]:
    words = content.split(r' ')
    words_in_one_line = []
    lines = []

    for word in words:
        if len(' '.join(words_in_one_line + [word])) > text_length:
            lines.append(' '.join(words_in_one_line))
            words_in_one_line.clear()

        words_in_one_line.append(word)

    if words_in_one_line:
        # Get the remainders.
        lines.append(' '.join(words_in_one_line))

    return lines


@contextmanager
def echo_progress(message: str, post_op_message: str, color: str):
    click.secho('>>> ' + message + ' ', nl=False)
    click.secho('IN PROGRESS', fg='yellow')
    try:
        yield
    except KeyboardInterrupt:
        click.secho('>>> ' + message + ' ', nl=False)
        click.secho('SKIPPED', fg='magenta')
    else:
        click.secho('>>> ' + message + ' ', nl=False)
        click.secho(post_op_message.upper(), fg=color)


def echo_dict_in_table(data: Dict[str, Any], left_padding_size: int = 0, cell_padding_size: int = 2):
    displayed_data = {
        k: json.dumps(v)
        for k, v in data.items()
        if v is not None
    }

    term_dimension = get_terminal_size((80, 24))
    term_width = term_dimension.columns

    key_length = max([len(k) for k in data.keys()]) + cell_padding_size
    max_value_length = term_width - left_padding_size - key_length

    left_padding = ' ' * left_padding_size

    print()
    for k, v in displayed_data.items():
        key_padding_length = key_length - len(k)

        displayed_value = v

        if len(displayed_value) > max_value_length:
            displayed_value = displayed_value[:max_value_length - 3] + '...'

        print(f'{left_padding}{k}{" " * key_padding_length}{displayed_value}')
    print()

def echo_list(title: str, items: List[str]):
    click.secho(title)
    for item in items:
        click.secho(f'  ● {item}')


def show_alternative_for_deprecated_command(alternative: Optional[str]):
    bg_color = 'yellow'
    fg_color = 'white'

    if alternative:
        echo_header(f'WARNING: Please use "{alternative}" instead.', bg_color, fg_color)
    else:
        echo_header('WARNING: No alternative to this command.', bg_color, fg_color)


def echo_result(prefix: Optional[str], result_color: str, result: str, message: str, emoji: Optional[str] = None,
                to_stderr: bool = True):
    if prefix:
        click.secho(f'[{prefix}]', dim=True, nl=False, err=True, bold=True)

    click.secho(f' {emoji} {result.upper()} ' if emoji else f' {result.upper()} ',
                fg=result_color,
                nl=False,
                err=to_stderr)
    click.secho(message, err=True)