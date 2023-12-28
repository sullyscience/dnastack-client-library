import random
import time
from abc import ABC
from typing import Dict, Optional, List, Any, Callable

from dnastack.common.logger import TraceableLogger
from dnastack.feature_flags import in_global_debug_mode


def _generate_random_64bit_string() -> str:
    """Returns a 64 bit UTF-8 encoded string. In the interests of simplicity,
    this is always cast to a `str` instead of (in py2 land) a unicode string.
    Certain clients (I'm looking at you, Twisted) don't enjoy unicode headers.

    This code is copied from https://github.com/Yelp/py_zipkin/blob/master/py_zipkin/util.py.

    :returns: random 16-character string
    """
    return f"{random.getrandbits(64):016x}"


def _generate_random_128bit_string() -> str:
    """Returns a 128 bit UTF-8 encoded string. Follows the same conventions
    as generate_random_64bit_string().

    The upper 32 bits are the current time in epoch seconds, and the
    lower 96 bits are random. This allows for AWS X-Ray `interop
    <https://github.com/openzipkin/zipkin/issues/1754>`_

    This code is copied from https://github.com/Yelp/py_zipkin/blob/master/py_zipkin/util.py.

    :returns: 32-character hex string
    """
    t = int(time.time())
    lower_96 = random.getrandbits(96)
    return f"{(t << 96) | lower_96:032x}"


class _SpanInterface(ABC):
    """ Interface for Distributed Tracing Span """
    def __init__(self):
        self._active: bool = True

        local_logger_name = f'Span(origin={self.origin})' if self.origin else 'Span'

        self._logger = TraceableLogger.make(local_logger_name,
                                            trace_id=self.trace_id,
                                            span_id=self.span_id)
        self._logger.debug('Begin')

    @property
    def active(self) -> bool:
        return self._active

    @property
    def origin(self) -> str:
        raise NotImplementedError()

    @property
    def parent(self):
        raise NotImplementedError()

    @property
    def trace_id(self) -> str:
        raise NotImplementedError()

    @property
    def span_id(self) -> str:
        raise NotImplementedError()

    @property
    def metadata(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def __enter__(self):
        assert self._active is not False, 'This span has already been deactivated.'
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def create_http_headers(self) -> Dict[str, str]:
        return {
            k: v
            for k, v in {
                'X-B3-TraceId': self.trace_id,
                'X-B3-ParentSpanId': self.parent.span_id if self.parent else None,
                'X-B3-SpanId': self.span_id,
                'X-B3-Sampled': '0',
            }.items()
            if v is not None
        }

    def create_span_logger(self, parent_logger: TraceableLogger) -> TraceableLogger:
        return parent_logger.fork(trace_id=self.trace_id, span_id=self.span_id)

    def close(self):
        self._active = False
        self._logger.debug('End')

        if in_global_debug_mode and not self.parent:
            self.print_tree(use_logger=True)

    def print_tree(self,
                   print_root: bool = True,
                   depth: int = 0,
                   indent: int = 2,
                   use_logger: bool = False):
        raise NotImplementedError()

    def __str__(self):
        attrs = [
            f'{k}={v}'
            for k, v in [
                ('trace_id', self.trace_id),
                ('span_id', self.span_id),
                ('parent_span_id', self.parent.span_id if self.parent else None),
                ('origin', self.origin),
                ('metadata', self.metadata),
            ]
            if v
        ]
        return f'Span({", ".join(attrs)})'


class Span(_SpanInterface):
    """ Distributed Tracing Span """
    def __init__(self,
                 trace_id: Optional[str] = None,
                 span_id: Optional[str] = None,
                 parent: Optional[_SpanInterface] = None,
                 origin: Any = None,
                 metadata: Optional[Dict[str, Any]] = None):
        self.__parent = parent
        self.__trace_id = (
                              parent.trace_id
                              if self.__parent is not None
                              else trace_id
                          ) or _generate_random_128bit_string()
        self.__span_id = span_id or _generate_random_64bit_string()
        self.__children: List[Span] = []
        self.__metadata = metadata

        # noinspection PyUnresolvedReferences
        self._origin = (
            self.__parent.origin
            if self.__parent
            else (
                origin
                if isinstance(origin, str)
                else f'{type(origin).__module__}.{type(origin).__name__}'
            )
        ) if origin else None

        super().__init__()

    @property
    def origin(self) -> str:
        return self._origin

    @property
    def parent(self):
        return self.__parent

    @property
    def trace_id(self) -> str:
        return self.__trace_id

    @property
    def span_id(self) -> str:
        return self.__span_id

    @property
    def metadata(self) -> Dict[str, Any]:
        return self.__metadata

    def new_span(self, metadata: Optional[Dict[str, Any]] = None) -> _SpanInterface:
        child_span = Span(self.trace_id, parent=self, metadata=metadata)
        self.__children.append(child_span)
        return child_span

    def print_tree(self,
                   print_root: bool = True,
                   depth: int = 0,
                   indent: int = 2,
                   external_printer: Optional[Callable[[str], None]] = None,
                   use_logger: bool = False):
        printer = external_printer or (self._logger.debug if use_logger else print)

        if print_root:
            printer(f'* {self}')

        next_depth = depth + 1

        for child_span in self.__children:
            printer((' ' * (next_depth * indent)) + f'* {child_span}')
            child_span.print_tree(print_root=False,
                                  depth=next_depth,
                                  external_printer=external_printer,
                                  use_logger=use_logger)
