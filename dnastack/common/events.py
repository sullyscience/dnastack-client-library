import hashlib
import logging
from copy import deepcopy
from dataclasses import dataclass, asdict
from pprint import pformat
from typing import Any, Dict, Optional, Callable, List, Union

from pydantic import Field, BaseModel

from dnastack.common.logger import get_logger


class Event(BaseModel):
    details: Dict[str, Any]
    propagated: bool = Field(default=True)

    def stop_propagation(self):
        self.propagated = False

    @classmethod
    def make(cls, details: Optional[Dict[str, Any]] = None):
        return Event(details=deepcopy(details) if details else dict())


class EventHandler:
    def __call__(self, event: Event) -> None:
        raise NotImplementedError()


class EventTypeNotRegistered(RuntimeError):
    """ Raised when the given event type is not registered """


class AbstractEventSource:
    def get_id(self) -> str:
        raise NotImplementedError()

    def get_fixed_types(self) -> List[str]:
        raise NotImplementedError()

    def add_fixed_types(self, *fixed_types):
        raise NotImplementedError()

    def dispatch(self, event_type: str, event: Union[Event, Dict[str, Any]]):
        raise NotImplementedError()

    def on(self, event_type: str, handler: Union[EventHandler, Callable[[Event], None]]):
        raise NotImplementedError()

    def off(self, event_type: str, handler: Union[EventHandler, Callable[[Event], None]]):
        raise NotImplementedError()

    def clear(self, event_type: Optional[str] = None):
        raise NotImplementedError()


class EventSource(AbstractEventSource):
    """
    Event Source

    This is not thread-safe.
    """

    def __init__(self, fixed_types: Optional[List[str]] = None, origin: Optional[Any] = None):
        self._origin = origin
        self._alias = f'{type(self).__name__}/{hash(self)}'

        if self._origin:
            if isinstance(self._origin, type(self)):
                self._alias = f'{self._origin}/{self._alias}'
            else:
                self._alias = f'{type(self._origin).__name__}/{hash(self._origin)}/{self._alias}'

        self._event_logger = get_logger(self._alias, logging.WARNING)
        self._event_handlers: Dict[str, List[Union[EventHandler, Callable[[Event], None]]]] = dict()
        self._fixed_types = fixed_types or list()

        self._event_logger.debug('Initialized')

    def get_id(self) -> str:
        return str(hash(self))

    def get_fixed_types(self) -> List[str]:
        return deepcopy(self._fixed_types)

    def add_fixed_types(self, *fixed_types):
        self._fixed_types.extend(fixed_types)

    def dispatch(self, event_type: str, event: Union[None, Event, Dict[str, Any]]):
        self._raise_error_for_non_registered_event_type(event_type)
        actual_event = event if isinstance(event, Event) else Event.make(details=event)

        event_logger = get_logger(
            f'{self._event_logger.name}/{event_type}/{self._compute_event_hash(actual_event)}/DISPATCH',
            self._event_logger.level
        )

        event_logger.debug(f'BEGIN')

        if event_type in self._event_handlers:
            for handler in self._event_handlers[event_type]:
                if not actual_event.propagated:
                    break
                event_logger.debug(f'INVOKE {handler}')
                handler(actual_event)
        else:
            pass

        event_logger.debug(f'END')

    def on(self, event_type: str, handler: Union[EventHandler, Callable[[Event], None]]):
        self._raise_error_for_non_registered_event_type(event_type)

        if event_type not in self._event_handlers:
            self._event_handlers[event_type] = list()

        if handler in self._event_handlers[event_type]:
            self._event_logger.debug(f'E/{event_type}: IGNORE BINDING {handler} (duplicate)')
        else:
            self._event_handlers[event_type].append(handler)
            self._event_logger.debug(f'E/{event_type}: BIND {handler}')

        return self

    def off(self, event_type: str, handler: Union[EventHandler, Callable[[Event], None]]):
        self._raise_error_for_non_registered_event_type(event_type)

        if event_type not in self._event_handlers:
            self._event_handlers[event_type] = list()

        new_handlers = [
            existing_handler
            for existing_handler in self._event_handlers[event_type]
            if hash(handler) != hash(existing_handler)
        ]

        self._event_handlers[event_type] = new_handlers
        self._event_logger.debug(f'E/{event_type}: UNBIND {handler}')

        return self

    def clear(self, event_type: Optional[str] = None):
        if event_type:
            if event_type in self._event_handlers:
                self._event_handlers[event_type].clear()
            else:
                pass
        else:
            self._event_handlers.clear()

    def _raise_error_for_non_registered_event_type(self, event_type: str):
        if self._fixed_types and event_type not in self._fixed_types:
            self._event_logger.error(
                f'Unknown event type {event_type}... The registered event type(s) is/are {self._fixed_types}')
            raise EventTypeNotRegistered(f'Given {event_type}, but expected {", ".join(self._fixed_types)}')

    def set_passthrough(self, origin: AbstractEventSource):
        self._event_logger.debug(f'SET PASSTHROUGH {origin} => {self}')

        for event_type in origin.get_fixed_types():
            self.relay_from(origin, event_type)

    def relay_from(self, origin: AbstractEventSource, event_type: str):
        self._event_logger.debug(f'SET RELAY ON {event_type}: {origin} => {self}')
        origin.on(event_type, EventRelay(self, event_type))

    @staticmethod
    def _compute_event_hash(obj: Event) -> str:
        h = hashlib.new('sha1')
        try:
            h.update(obj.json().encode('utf-8'))
        except TypeError:
            h.update(str(obj.dict()).encode('utf-8'))
        return h.hexdigest()[:8]

    def __repr__(self):
        return self._alias


class EventRelay(EventHandler):
    def __init__(self, relay_source: AbstractEventSource, event_type: str):
        self.__relay_source = relay_source
        self.__event_type = event_type
        self.__logger = get_logger(f'{type(self).__name__}/{self.__relay_source.get_id()}/{self.__event_type}',
                                   logging.WARNING)

    def __call__(self, event: Event) -> None:
        self.__logger.debug('Relaying...')
        self.__relay_source.dispatch(self.__event_type, event)
        self.__logger.debug('Relayed')
