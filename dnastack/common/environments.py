import json
import logging
import os
from sys import stderr

from typing import Any, Callable, Optional, Set

__shown_env_description_list: Set[str] = set()

# Initialize the logger specifically for this module. It is not designed to be used by other modules. If you need
# a logger, please check out "get_logger" or any functions or TraceableLogger's "make" method.
__log_format = '[ %(asctime)s | %(levelname)s ] %(name)s: %(message)s'
__log_level = logging.DEBUG if str(os.getenv('DNASTACK_DEBUG') or '').lower() in ['1', 'true'] else logging.INFO
__log_formatter = logging.Formatter(__log_format)

__log_handler = logging.StreamHandler(stderr)
__log_handler.setLevel(__log_level)
__log_handler.setFormatter(__log_formatter)

__env_logger = logging.Logger('environment', level=__log_level)
__env_logger.setLevel(__log_level)
__env_logger.addHandler(__log_handler)


def __boolean_flag(v: str):
    return str(v or '').lower() in ['1', 'true']


class EnvironmentVariableRequired(RuntimeError):
    def __init__(self, environment_variable_name: str, hint: Optional[str]):
        feedback = f'Environment variable required: {environment_variable_name}'

        if hint:
            feedback += f' ({hint})'

        super(EnvironmentVariableRequired, self).__init__(feedback)


def env(key: str,
        default: Any = None,
        required: bool = False,
        transform: Optional[Callable] = None,
        hint: Optional[str] = None,
        env_type: Optional[str] = None,
        description: Optional[str] = None) -> Any:
    if key not in os.environ and required:
        __env_logger.error(f'❌ Missing {(env_type or "var").upper()} "{key}" ({description})')
        raise EnvironmentVariableRequired(key, hint)

    original_value = os.getenv(key)

    returning_value = (
        (default or original_value)
        if original_value is None
        else (transform(original_value) if transform else original_value)
    )

    if key not in __shown_env_description_list:
        __shown_env_description_list.add(key)
        json_value = json.dumps(returning_value)

        if description:
            __env_logger.debug(f'💡 {(env_type or "env").upper()} "{key}" ({description}) → {json_value}')
        else:
            __env_logger.debug(f'💡 {(env_type or "env").upper()} "{key}" → {json_value}')

    return returning_value


def flag(key: str, description: Optional[str] = None) -> bool:
    return bool(env(key, default=False, transform=__boolean_flag, env_type='flag', description=description))
