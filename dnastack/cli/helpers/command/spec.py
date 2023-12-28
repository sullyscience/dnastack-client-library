import re

from typing import List, Optional, Union, Any, Type

from pydantic import BaseModel, Field

from dnastack.cli.helpers.iterator_printer import OutputFormat


class ArgumentSpec(BaseModel):
    """
    Argument specification

    This is designed to use with @command where you want to customize how it automatically maps the callable's arguments
    as the command arguments/options.
    """
    name: str
    arg_names: Optional[List[str]] = Field(default_factory=list)
    as_option: bool = None
    help: Optional[str] = None
    choices: Optional[List] = Field(default_factory=list)
    ignored: bool = False
    nargs: Optional[Union[int, str]] = None
    type: Optional[Type] = None  # WARNING: This will override the parameter reflection.
    default: Optional[Any] = None  # WARNING: This will override the parameter reflection.
    required: Optional[bool] = None  # WARNING: This will override the parameter reflection.

    # NOTE: the "type" and "default value" can be determined via the reflection if implemented.

    def get_argument_names(self) -> List[str]:
        if not self.arg_names:
            return self.convert_param_name_to_argument_names(self.name, self.as_option)
        else:
            return [*self.arg_names, self.name]

    @staticmethod
    def convert_param_name_to_argument_names(param_name: str, as_option: bool = False) -> List[str]:
        if as_option:
            return [f"--{re.sub(r'_', '-', param_name)}", param_name]
        else:
            return [param_name]


SINGLE_ENDPOINT_ID_SPEC = ArgumentSpec(
    name='endpoint_id',
    arg_names=['--endpoint-id'],
    as_option=True,
    help='Endpoint ID',
    required=False,
)

MULTIPLE_ENDPOINT_ID_SPEC = ArgumentSpec(
    name='endpoint_id',
    arg_names=['--endpoint-id'],
    as_option=True,
    help='Endpoint IDs, separated by comma, e.g., --endpoint-id=s_1,s_2,...,s_n',
    required=False,
)

RESOURCE_OUTPUT_SPEC = ArgumentSpec(
    name='output',
    arg_names=['--output', '-o'],
    as_option=True,
    choices=[OutputFormat.JSON, OutputFormat.YAML],
    help='Output format',
    default=OutputFormat.YAML,
    required=False,
)

DATA_OUTPUT_SPEC = ArgumentSpec(
    name='output',
    arg_names=['--output', '-o'],
    as_option=True,
    choices=[OutputFormat.CSV, OutputFormat.JSON, OutputFormat.YAML],
    help='Output format',
    default=OutputFormat.JSON,
    required=False,
)
