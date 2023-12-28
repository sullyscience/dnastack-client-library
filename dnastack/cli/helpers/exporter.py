import csv
from json import dumps

import csv
import datetime
import io
import yaml
from decimal import Decimal
from pydantic import BaseModel
from typing import Any, List, Type, Optional

from dnastack.client.result_iterator import ResultIterator


class ConversionError(RuntimeError):
    """ Raised when the data conversion fails """


def normalize(content: Any, map_decimal: Type = str, sort_keys: bool = True) -> Any:
    """
    Normalize the content for data export

    .. note:: This is not designed for two-way data conversion.
    """
    if isinstance(content, Decimal):
        return map_decimal(content)
    elif isinstance(content, BaseModel):
        return normalize(content.dict(), map_decimal=map_decimal)
    elif isinstance(content, dict):
        # Handle a dictionary
        DEFAULT_WEIGHT = 99999
        FIXED_WEIGHTS = {
            'id': 0,
            'name': 2,
            'slugName': 2,
            'run_id': 1,
        }

        properties = (
            sorted(content.keys(),
                   key=lambda k: f'{FIXED_WEIGHTS.get(k) if k in FIXED_WEIGHTS else DEFAULT_WEIGHT:0>8}//{k}')
            if sort_keys
            else list(content.keys())
        )

        return {
            p_name: normalize(content[p_name], map_decimal=map_decimal)
            for p_name in properties
        }
    elif isinstance(content, (tuple, list, set, ResultIterator)):
        # Handle a list or tuple or set or anything iterable
        return [normalize(i, map_decimal=map_decimal) for i in content]
    elif (isinstance(content, datetime.datetime)
          or isinstance(content, datetime.date)
          or isinstance(content, datetime.time)):
        return content.isoformat()
    else:
        return content


def to_json(content: Any, indent: Optional[int] = 2):
    try:
        return dumps(content, indent=indent)
    except Exception:
        raise ConversionError(f'Failed to convert:\n\n{content}\n\nas JSON string')


def to_yaml(content: Any, indent: Optional[int] = 2):
    try:
        return yaml.dump(content, Dumper=yaml.SafeDumper, indent=indent)
    except Exception:
        raise ConversionError(f'Failed to convert:\n\n{content}\n\nas YAML string')


def to_csv(object_list: List[dict]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)

    # if we have at least one result, add the headers
    if len(object_list) > 0:
        writer.writerow(object_list[0].keys())

    for res in object_list:
        data_row = list(map(lambda x: str(x).replace(",", r'\\,'), res.values()))
        writer.writerow(data_row)

    return output.getvalue()
