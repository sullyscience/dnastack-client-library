# Enable logging for "requests"
import logging
from http.client import HTTPConnection
from sys import stderr
from traceback import print_stack
from typing import Optional

from dnastack.common.environments import env
from dnastack.feature_flags import in_global_debug_mode

logging_format = '[ %(asctime)s | %(levelname)s ] %(name)s: %(message)s'
overriding_logging_level_name = env(
    'DNASTACK_LOG_LEVEL',
    description='Default CLI/library log level. In the debug mode, the log level will be overridden to DEBUG',
    required=False
)
default_logging_level = getattr(logging, overriding_logging_level_name) \
    if overriding_logging_level_name in ('DEBUG', 'INFO', 'WARNING', 'ERROR') \
    else logging.WARNING

if in_global_debug_mode:
    default_logging_level = logging.DEBUG
    HTTPConnection.debuglevel = 1

logging.basicConfig(format=logging_format,
                    level=default_logging_level)

# Configure the logger of HTTP client (global settings)
requests_log = logging.getLogger("urllib3")
requests_log.setLevel(default_logging_level)
requests_log.propagate = True


class TraceableLogger(logging.Logger):
    def __init__(self, name, level=logging.NOTSET, trace_id: Optional[str] = None, span_id: Optional[str] = None):
        super().__init__(name, level)

        self.actual_name = name
        self.trace_id = trace_id
        self.span_id = span_id

        # Override the name of the logger.
        if self.trace_id and self.span_id:
            self.name = f'{self.actual_name},{self.trace_id},{self.span_id}'

    def fork(self, level: Optional[int] = None, trace_id: Optional[str] = None, span_id: Optional[str] = None):
        return self.make(self.actual_name, level or self.level, trace_id, span_id)

    @classmethod
    def make(cls, name, level: Optional[int] = None, trace_id: Optional[str] = None, span_id: Optional[str] = None):
        log_level = level or default_logging_level

        formatter = logging.Formatter(logging_format)

        handler = logging.StreamHandler(stderr)
        handler.setLevel(log_level)
        handler.setFormatter(formatter)

        logger = cls(name, level=log_level, trace_id=trace_id, span_id=span_id)
        logger.setLevel(log_level)
        logger.addHandler(handler)

        return logger


def get_logger(name: str, level: Optional[int] = None) -> TraceableLogger:
    return TraceableLogger.make(name, level)


def get_logger_for(ref: object,
                   level: Optional[int] = None) -> TraceableLogger:
    """ Shortcut for creating a logger of a class/object

        Set use_fqcn to True if you want the name of the logger to be the fully qualified class name.
        Otherwise, it will use just the class name by default.

        Set metadata if you need to inject more information to the logger name.
    """
    logger_name = f'{type(ref).__module__}.{type(ref).__name__}'

    return TraceableLogger.make(logger_name, level)


def alert_for_deprecation(message: str):
    l = get_logger('DEPRECATED')
    l.warning(message)
    print_stack()
