import os
from typing import Optional

from dnastack.cli.helpers.iterator_printer import show_iterator
from dnastack.client.data_connect import DataConnectClient


def handle_query(data_connect: DataConnectClient,
                 query: str,
                 decimal_as: str = 'string',
                 no_auth: bool = False,
                 output_format: Optional[str] = None,
                 allow_using_query_from_file: bool = False):
    actual_query = query

    if allow_using_query_from_file:
        if query.startswith('@'):
            query_file_path = query[1:]
            if os.path.exists(query_file_path):
                with open(query_file_path, 'r') as f:
                    actual_query = f.read()
            else:
                raise IOError(f'File not found: {query_file_path}')

    iterator = data_connect.query(actual_query, no_auth=no_auth)
    show_iterator(output_format, iterator, decimal_as=decimal_as, sort_keys=False)
