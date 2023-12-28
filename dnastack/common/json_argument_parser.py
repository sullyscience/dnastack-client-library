import json
import traceback
from enum import Enum
from io import UnsupportedOperation
from typing import List, Dict, Union

from dnastack.common.logger import get_logger
from dnastack.feature_flags import in_global_debug_mode

logger = get_logger('json_argument_parser')

try:
    from httpie.cli.argtypes import KeyValueArgType
    from httpie.cli.constants import *
    from httpie.cli.nested_json import interpret_nested_json
except UnsupportedOperation as e:
    # NOTE This is just to bypass the error raised by Colab's Jupyter Notebook.
    # FIXME Fix the issue where ncurses raises io.UnsupportedOperation.
    if in_global_debug_mode:
        logger.warning("Could start importing httpie modules but failed to finish it.")
        traceback.print_exc()
        logger.warning("This module will be imported but there will be no guarantee that the dependant code will work normally.")


JSONType = Union[str, bool, int, list, dict]
KV_PAIR_SEPARATOR = ","


class ArgumentType(str, Enum):
    JSON_LITERAL_PARAM_TYPE = "JSON_LITERAL"
    FILE = "FILE"
    KV_PARAM_TYPE = "KEY_VALUE"
    UNKNOWN_PARAM_TYPE = "UNKNOWN"


class FileOrValue:

    def __init__(self, raw_value: str):
        self._raw_value = raw_value
        self._argument_type = get_argument_type(raw_value)

    @property
    def raw_value(self):
        return self._raw_value

    @property
    def argument_type(self):
        return self._argument_type

    def value(self) -> str:
        loaded_value = self.raw_value
        if self.argument_type == ArgumentType.FILE:
            loaded_value = read_file_content(self.raw_value)
        return loaded_value


class JsonLike(FileOrValue):
    def parsed_value(self) -> JSONType:
        value = super().value()
        if self.argument_type == ArgumentType.KV_PARAM_TYPE:
            return parse_kv_arguments(split_kv_pairs(value))
        else:
            return json.loads(value)


def merge(base, override_dict, path=None):
    """
    merges b into a
    """
    if path is None:
        path = []
    for key in override_dict:
        if key in base:
            if isinstance(base[key], dict) and isinstance(override_dict[key], dict):
                merge(base[key], override_dict[key], path + [str(key)])
            elif base[key] == override_dict[key]:
                pass  # same leaf value
            else:
                # o
                base[key] = override_dict[key]
        else:
            base[key] = override_dict[key]
    return base


def split_kv_pairs(kv_pairs: str) -> List[str]:
    kv_pairs = kv_pairs.replace("\\,", "%2C")
    return [kv_pair.replace("\\,", ",").replace("%2C", ",") for kv_pair in kv_pairs.split(KV_PAIR_SEPARATOR)]


def is_json_object_or_array_string(string: str) -> bool:
    try:
        json_val = json.loads(string)
        return isinstance(json_val, list) or isinstance(json_val, dict)
    except ValueError as e:
        return False


def read_file_content(argument: str) -> str:
    argument = argument.replace("@", "", 1)
    with open(argument) as argument_fp:
        return argument_fp.read()


def get_argument_type(argument: str) -> str:
    if not argument:
        return ArgumentType.UNKNOWN_PARAM_TYPE
    if argument.startswith("@"):
        return ArgumentType.FILE
    if is_json_object_or_array_string(argument):
        return ArgumentType.JSON_LITERAL_PARAM_TYPE
    if "=" in argument:
        return ArgumentType.KV_PARAM_TYPE
    return ArgumentType.UNKNOWN_PARAM_TYPE


def parse_kv_arguments(arguments: List[str]) -> Union[List[JSONType], Dict[str, JSONType]]:
    arg_types = KeyValueArgType(*SEPARATOR_GROUP_NESTED_JSON_ITEMS)
    kv_pairs = list()
    for argument in arguments:
        arg_type = arg_types(argument)
        if arg_type.sep == SEPARATOR_DATA_EMBED_FILE_CONTENTS:
            with open(arg_type.value) as arg_fp:
                arg_type.value = arg_fp.read()
        elif arg_type.sep == SEPARATOR_DATA_EMBED_RAW_JSON_FILE:
            with open(arg_type.value) as arg_fp:
                arg_type.value = json.load(arg_fp)
        elif arg_type.sep == SEPARATOR_DATA_RAW_JSON:
            arg_type.value = json.loads(arg_type.value)
        kv_pairs.append((arg_type.key, arg_type.value))
    nested_json = interpret_nested_json(kv_pairs)
    # If the value being specified was a root list, we want to extract the list
    if len(nested_json.keys()) == 1 and '' in nested_json.keys():
        nested_json = nested_json['']
    return nested_json


def parse_and_merge_arguments(arguments: List[JsonLike]) -> Dict[str, JSONType]:
    arguments_results = dict()
    kv_arguments = list()
    for argument in arguments:
        if argument.argument_type == ArgumentType.KV_PARAM_TYPE:
            kv_arguments.extend(split_kv_pairs(argument.value()))
        elif argument.argument_type == ArgumentType.UNKNOWN_PARAM_TYPE:
            raise ValueError(f"Cannot merge non json value from argument: {argument}")
        else:
            parsed = argument.parsed_value()
            merge(arguments_results, parsed)
    kv_arguments_result = parse_kv_arguments(kv_arguments)
    if kv_arguments_result:
        merge(arguments_results, kv_arguments_result)
    return arguments_results
