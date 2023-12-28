########################################
# Command and Specification Definition #
########################################
import re

import logging

import sys
from click import Group

from traceback import print_exc

from typing import List, Union, Callable, Dict, Any, Optional

import inspect

import click

from dnastack.cli.helpers.command.spec import ArgumentSpec, SINGLE_ENDPOINT_ID_SPEC
from dnastack.common.logger import get_logger
from dnastack.common.tracing import Span
from dnastack.feature_flags import in_global_debug_mode, show_distributed_trace_stack_on_error

DEFAULT_SPECS = [
    ArgumentSpec(
        name='no_auth',
        arg_names=['--no-auth'],
        as_option=True,
        help='Disable authentication',
        required=False,
    ),
    SINGLE_ENDPOINT_ID_SPEC,
    ArgumentSpec(
        name='context',
        arg_names=['--context'],
        as_option=True,
        help='Context',
        required=False,
    ),
]


def command(command_group: Group,
            alternate_command_name: Optional[str] = None,
            specs: List[Union[ArgumentSpec, Dict[str, Any]]] = None,
            excluded_arguments: List[str] = None,
            setup_debug_enabled: bool = False,
            hidden: bool = False):
    """
    Set up a basic command and automatically configure CLI arguments or options based on the signature
    of the handler (given callable).

    :param command_group: the command group
    :param alternate_command_name: the alternate command name - by default, the command name is derived from the name
                                   of the annotated/decorated callable.
    :param specs: OVERRIDING argument/option specifications - by default, this decorator will automatically set any
                  callable's arguments as CLI in-line arguments.
    :param excluded_arguments: The list of callable's arguments to ignore from the autoconfiguration.
    """
    _logger = get_logger('@command', logging.DEBUG) if setup_debug_enabled else get_logger('@command', logging.WARNING)

    argument_specs = [(ArgumentSpec(**spec) if isinstance(spec, dict) else spec)
                      for spec in (specs or list())]

    for default_spec in DEFAULT_SPECS:
        if [argument_spec for argument_spec in argument_specs if argument_spec.name == default_spec.name]:
            continue
        else:
            argument_specs.append(default_spec)

    argument_spec_map: Dict[str, ArgumentSpec] = {spec.name: spec for spec in argument_specs}

    excluded_argument_names = excluded_arguments or list()
    for spec in argument_spec_map.values():
        if spec.ignored:
            excluded_argument_names.append(spec.name)

    def decorator(handler: Callable):
        command_name = alternate_command_name if alternate_command_name else re.sub(r'_', '-', handler.__name__)

        _decorator_logger = get_logger(f'{_logger.name}/{command_name}', _logger.level)

        handler_signature = inspect.signature(handler)

        def handle_invocation(*args, **kwargs):
            if in_global_debug_mode:
                # In the debug mode, no error will be handled gracefully so that the developers can see the full detail.
                handler(*args, **kwargs)
            else:
                try:
                    handler(*args, **kwargs)
                except (IOError, TypeError, AttributeError, IndexError, KeyError) as e:
                    click.secho('Unexpected programming error', fg='red', err=True)

                    print_exc()

                    raise SystemExit(1) from e
                except Exception as e:
                    error_type = type(e).__name__
                    # error_type = re.sub(r'([A-Z])', r' \1', error_type).strip()
                    # error_type = re.sub(r' Error$', r'', error_type).strip().capitalize()

                    click.secho(f'{error_type}: ', fg='red', bold=True, nl=False, err=True)
                    click.secho(e, fg='red', err=True)

                    if hasattr(e, 'trace'):
                        trace: Span = e.trace
                        click.secho(f'Incident ID {trace.trace_id}', dim=True, err=True)

                        def _printer(msg: str):
                            click.secho(msg, dim=True, err=True)

                        if show_distributed_trace_stack_on_error:
                            trace.print_tree(external_printer=_printer)

                    raise SystemExit(1) from e

        handle_invocation.__doc__ = handler.__doc__

        command_obj = command_group.command(command_name, hidden=hidden)(handle_invocation)

        for param_name, param in handler_signature.parameters.items():
            if param_name in excluded_argument_names:
                continue

            _decorator_param_logger = get_logger(f'{_decorator_logger.name}/{param_name}', _logger.level)

            required = True
            default_value = None
            help_text = None
            nargs = None
            as_option = False
            as_flag = False

            _decorator_param_logger.debug(f'REFLECTED: {param}')

            annotation = param.annotation
            if annotation is None or annotation == inspect._empty:
                param_type = str
            elif inspect.isclass(annotation):
                param_type = annotation
            else:
                if sys.version_info >= (3, 8):
                    ##################################
                    # To support Python 3.8 or newer #
                    ##################################

                    from typing import get_origin, get_args
                    special_type = get_origin(annotation)
                    type_args = get_args(annotation)

                    _decorator_param_logger.debug(
                        f'SPECIAL TYPE: special_type = {special_type}, type_args = {type_args}')

                    if special_type is Union:
                        param_type = [t for t in type_args if t is not None][0]
                        required = type(None) not in type_args
                    elif special_type is list or special_type is List:
                        param_type = [t for t in type_args if t is not None][0]
                        required = True
                        nargs = -1
                    else:
                        raise RuntimeError(f'Programming Error: The type of parameter {param_name} ({annotation}) is '
                                           f'not supported by this decorator. Please contact the technical support.')
                else:
                    ##################################
                    # To support Python 3.7 or older #
                    ##################################

                    if str(annotation).startswith('typing.Union[') and 'NoneType' in str(annotation):
                        # To keep this simple, the union annotation with none type is assumed
                        # to be for an optional string argument. Python 3.8 code branch can
                        # detect the type better.
                        # We detect the bool type to remove need for explicit boolean value.
                        # E.g. --status true vs --status
                        if 'bool' in str(annotation):
                            param_type = bool
                        else:
                            param_type = str
                        required = False
                    elif str(annotation) == 'typing.List[str]':
                        param_type = str
                        required = True
                        nargs = -1
                    else:
                        raise RuntimeError(f'Programming Error: The type of parameter {param_name} ({annotation}) is '
                                           f'not supported by @command. Please contact the technical support.')

            if param.default != inspect._empty:
                default_value = param.default
                required = False

            additional_specs = dict(type=param_type,
                                    required=required,
                                    default=default_value,
                                    show_default=not required and default_value is not None)

            if param_type is bool:
                as_option = True
                as_flag = True

            input_names = ArgumentSpec.convert_param_name_to_argument_names(param_name, as_option)

            # If the argument spec is defined, use the spec to override the reflection.
            if param_name in argument_spec_map:
                spec = argument_spec_map[param_name]

                _decorator_param_logger.debug(f'SPEC: {spec}')

                if spec.required is not None:
                    required = spec.required
                else:
                    spec.required = required

                if spec.as_option is not None:
                    as_option = spec.as_option
                else:
                    spec.as_option = as_option

                input_names = spec.get_argument_names()

                if spec.help:
                    help_text = spec.help

                if spec.choices:
                    additional_specs.update({
                        'type': click.Choice(spec.choices),
                        'show_choices': True,
                    })

                if spec.type:
                    additional_specs.update({
                        'type': spec.type
                    })

                if spec.default:
                    additional_specs.update({
                        'default': spec.default
                    })

                if spec.nargs:
                    nargs = spec.nargs

                additional_specs.update({
                    'required': required,
                    'show_default': not required and default_value is not None,
                })
            # END: spec overriding

            if as_option:
                if nargs is not None:
                    additional_specs['multiple'] = True
                    additional_specs['required'] = False
                    del additional_specs['default']

                if help_text:
                    additional_specs['help'] = help_text

                if as_flag:
                    additional_specs['is_flag'] = True
                    additional_specs['required'] = False
                    additional_specs['show_default'] = False

                _decorator_param_logger.debug(f'SET: Option ({input_names}, {additional_specs})')
                click.option(*input_names, **additional_specs)(command_obj)
            else:
                if nargs is not None:
                    additional_specs['nargs'] = nargs
                    additional_specs['required'] = False
                    del additional_specs['default']

                del additional_specs['show_default']

                _decorator_param_logger.debug(f'SET: Argument ({input_names}, {additional_specs})')
                try:
                    click.argument(*input_names, **additional_specs)(command_obj)
                except TypeError:
                    basic_error_feedback = (
                        f'Programming Error: Failed to set up a command argument with '
                        f'input_names={input_names} and additional_specs={additional_specs}.'
                    )
                    if bool([i for i in input_names if i.startswith('-')]):
                        basic_error_feedback += f' It seems the command is supposed to be an option. Please try to ' \
                                                f'set as_option=True to the argument specification for "{spec.name}"' \
                                                f'to resolve this error.'

                    raise RuntimeError(basic_error_feedback)
        # END: argument/option setup

        _decorator_logger.debug(f'Setup complete')

        return command_obj

    return decorator
