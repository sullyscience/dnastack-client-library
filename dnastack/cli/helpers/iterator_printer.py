import sys

import csv
from json import dumps as to_json_string

import click
from pydantic import BaseModel
from typing import TypeVar, Any, Iterable, Optional, Callable
from yaml import dump as to_yaml_string, SafeDumper

from dnastack.cli.helpers.exporter import normalize
from dnastack.feature_flags import in_interactive_shell, cli_show_list_item_index

I = TypeVar('I')
R = TypeVar('R')


def show_iterator(output_format: str,
                  iterator: Iterable[Any],
                  transform: Optional[Callable[[I], R]] = None,
                  limit: Optional[int] = None,
                  item_marker: Optional[Callable[[Any], Optional[str]]] = None,
                  decimal_as: str = 'string',
                  sort_keys: bool = True) -> int:
    """ Display the result from the iterator """
    if output_format == OutputFormat.JSON:
        printer = JsonIteratorPrinter()
    elif output_format == OutputFormat.YAML:
        printer = YamlIteratorPrinter()
    elif output_format == OutputFormat.CSV:
        printer = CsvIteratorPrinter()
    else:
        raise ValueError(f'The given output format ({output_format}) is not available.')

    return printer.print(iterator,
                         transform=transform,
                         limit=limit,
                         item_marker=item_marker,
                         decimal_as=decimal_as,
                         sort_keys=sort_keys)


class OutputFormat:
    JSON = 'json'
    YAML = 'yaml'
    CSV = 'csv'

    DEFAULT_FOR_RESOURCE = YAML
    DEFAULT_FOR_DATA = JSON


class BaseIteratorPrinter:
    def print(self,
              iterator: Iterable[Any],
              transform: Optional[Callable[[I], R]] = None,
              limit: Optional[int] = None,
              item_marker: Optional[Callable[[Any], Optional[str]]] = None,
              decimal_as: str = 'string',
              sort_keys: bool = True):
        raise NotImplementedError()


class JsonIteratorPrinter(BaseIteratorPrinter):
    def print(self,
              iterator: Iterable[Any],
              transform: Optional[Callable[[I], R]] = None,
              limit: Optional[int] = None,
              item_marker: Optional[Callable[[Any], Optional[str]]] = None,  # NOTE: Declared but ignored
              decimal_as: str = 'string',
              sort_keys: bool = True) -> int:
        row_count = 0

        for row in iterator:
            if limit and row_count >= limit:
                break

            if row_count == 0:
                # First row
                click.echo('[')
            else:
                click.echo(',', nl=False)

                if in_interactive_shell and cli_show_list_item_index:
                    click.secho(f' # {row_count}', dim=True, err=True, nl=False)

                click.echo('')  # just a new line

            entry = transform(row) if transform else row
            normalized = normalize(entry, map_decimal=str if decimal_as == 'string' else float, sort_keys=sort_keys)
            encoded = to_json_string(normalized, indent=2, sort_keys=False)

            click.echo(
                '\n'.join([
                    f'  {line}'
                    for line in encoded.split('\n')
                ]),
                nl=False
            )

            row_count += 1

        if row_count == 0:
            click.echo('[]')
        else:
            click.echo('\n]')

        return row_count


class CsvIteratorPrinter(BaseIteratorPrinter):
    def print(self,
              iterator: Iterable[Any],
              transform: Optional[Callable[[I], R]] = None,
              limit: Optional[int] = None,
              item_marker: Optional[Callable[[Any], Optional[str]]] = None,  # NOTE: Declared but ignored
              decimal_as: str = 'string',
              sort_keys: bool = True  # NOTE: Declared but ignored
              ) -> int:
        row_count = 0

        writer = csv.writer(sys.stdout)
        headers = []

        for row in iterator:
            if limit and row_count >= limit:
                break

            entry = transform(row) if transform else row
            normalized = normalize(entry, map_decimal=str if decimal_as == 'string' else float, sort_keys=sort_keys)

            if row_count == 0:
                headers.extend(normalized.keys())
                writer.writerow(headers)

            writer.writerow([normalized[h] if h in normalized else None for h in headers])

            row_count += 1

        return row_count


class YamlIteratorPrinter(BaseIteratorPrinter):
    def print(self,
              iterator: Iterable[Any],
              transform: Optional[Callable[[I], R]] = None,
              limit: Optional[int] = None,
              item_marker: Optional[Callable[[Any], Optional[str]]] = None,
              decimal_as: str = 'string',
              sort_keys: bool = True  # NOTE: Declared but ignored
              ) -> int:
        row_count = 0

        for row in iterator:
            if limit and row_count >= limit:
                break

            entry = transform(row) if transform else row
            normalized = normalize(entry)
            encoded = (
                normalized
                if isinstance(normalized, str)
                else to_yaml_string(normalize(entry),
                                    Dumper=SafeDumper,
                                    sort_keys=False)
            )

            click.echo('- ', nl=False)
            click.echo(
                '\n'.join([
                    f'  {line}'
                    for line in encoded.split('\n')
                ]).strip(),
                nl=False
            )

            if in_interactive_shell and item_marker:
                marker = item_marker(row)
                if marker:
                    click.secho(f' # {marker}', fg='green', err=True, nl=False)

            if in_interactive_shell and cli_show_list_item_index:
                click.secho(f' # {row_count}', dim=True, err=True, nl=False)

            click.echo()

            row_count += 1

        if row_count == 0:
            click.echo('[]')

        return row_count
