from abc import ABC, abstractmethod

import re
from uuid import uuid4

import requests
from imagination.decorator import service
from pydantic import BaseModel
from typing import Optional, List, Dict
from urllib.parse import urljoin, urlparse

from dnastack.common.auth_manager import AuthManager
from dnastack.client.factory import EndpointRepository
from dnastack.client.models import ServiceEndpoint
from dnastack.client.service_registry.client import ServiceRegistry, STANDARD_SERVICE_REGISTRY_TYPE_V1_0
from dnastack.client.service_registry.manager import ServiceRegistryManager
from dnastack.common.events import EventSource, Event
from dnastack.common.logger import get_logger
from dnastack.configuration.manager import ConfigurationManager
from dnastack.context.models import Context


class ContextMetadata(BaseModel):
    name: str
    selected: bool


class InvalidServiceRegistryError(RuntimeError):
    pass


class ContextMap(ABC):
    def __init__(self):
        self._logger = get_logger(type(self).__name__ + '/' + str(uuid4()))

    @abstractmethod
    def all(self) -> Dict[str, Context]:
        raise NotImplementedError()

    @abstractmethod
    def set_current_context_name(self, context_name: str):
        raise NotImplementedError()

    @property
    @abstractmethod
    def current_context_name(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def current_context(self) -> Optional[Context]:
        raise NotImplementedError()

    @abstractmethod
    def get(self, context_name: str) -> Context:
        raise NotImplementedError()

    @abstractmethod
    def set(self, context_name: str, context: Context):
        raise NotImplementedError()

    @abstractmethod
    def unset(self, context_name: str):
        raise NotImplementedError()

    @abstractmethod
    def rename(self, old_name: str, new_name: str):
        raise NotImplementedError()

    @abstractmethod
    def list(self) -> List[ContextMetadata]:
        raise NotImplementedError()


@service.registered()
class InMemoryContextMap(ContextMap):
    def __init__(self, reference_contexts: Optional[Dict[str, Context]] = None):
        super().__init__()

        self._current_context_name: Optional[str] = None

        # Internal context maps
        self._reference_contexts: Dict[str, Context] = reference_contexts or dict()

    def all(self) -> Dict[str, Context]:
        return self._reference_contexts

    def set_current_context_name(self, context_name: str):
        self._current_context_name = context_name

    @property
    def current_context_name(self):
        return self._current_context_name

    @property
    def current_context(self) -> Optional[Context]:
        return self._reference_contexts[self._current_context_name] if self._current_context_name else None

    def get(self, context_name: str) -> Context:
        return self._reference_contexts.get(context_name)

    def set(self, context_name: str, context: Context):
        # assert context_name not in self._reference_contexts, f'The context, called "{context_name}", already exists.'
        self._reference_contexts[context_name] = context

    def unset(self, context_name: str):
        assert context_name in self._reference_contexts, f'The context, called "{context_name}", does not exist.'

        del self._reference_contexts[context_name]

        if self._current_context_name == context_name:
            self._current_context_name = None

    def rename(self, old_name: str, new_name: str):
        assert old_name in self._reference_contexts, f'The context, called "{old_name}", does not exist.'
        assert new_name not in self._reference_contexts, f'The context, called "{new_name}", already exists.'

        self._reference_contexts[new_name] = self._reference_contexts[old_name]

        if self._current_context_name == old_name:
            self._current_context_name = new_name

    def list(self) -> List[ContextMetadata]:
        return [
            ContextMetadata(name=context_name,
                            selected=(self._current_context_name == context_name))
            for context_name in self._reference_contexts.keys()
        ]


@service.registered()
class ConfigurationBasedContextMap(ContextMap):
    def __init__(self, config_manager: ConfigurationManager):
        super().__init__()
        self.__config_manager = config_manager

    def all(self) -> Dict[str, Context]:
        return self._reference_contexts

    def set_current_context_name(self, context_name: str):
        config = self.__config_manager.load()
        config.current_context = context_name
        self.__config_manager.save(config)

    @property
    def current_context_name(self):
        return self.__config_manager.load().current_context

    @property
    def current_context(self) -> Optional[Context]:
        return self.get()

    def get(self, context_name: Optional[str] = None) -> Context:
        config = self.__config_manager.load()
        return config.contexts.get(context_name or config.current_context)

    def set(self, context_name: str, context: Context):
        config = self.__config_manager.load()
        config.contexts[context_name] = context
        self.__config_manager.save(config)

    def unset(self, context_name: str):
        config = self.__config_manager.load()

        assert context_name in config.contexts, f'The context, called "{context_name}", does not exist.'

        del config.contexts[context_name]

        if config.current_context == context_name:
            config.current_context = None

        self.__config_manager.save(config)

    def rename(self, old_name: str, new_name: str):
        config = self.__config_manager.load()

        assert old_name in config.contexts, f'The context, called "{old_name}", does not exist.'
        assert new_name not in config.contexts, f'The context, called "{new_name}", already exists.'

        config.contexts[new_name] = config.contexts[old_name]
        del config.contexts[old_name]

        if config.current_context == old_name:
            config.current_context = new_name

        self.__config_manager.save(config)

    def list(self) -> List[ContextMetadata]:
        config = self.__config_manager.load()
        return [
            ContextMetadata(name=context_name,
                            selected=(config.current_context == context_name))
            for context_name in config.contexts.keys()
        ]


class BaseContextManager:
    _re_http_scheme = re.compile(r'^https?://')
    _logger = get_logger('BaseContextManager')

    __propagated_auth_event_types = [
        'auth-begin',
        'auth-end',
        'no-refresh-token',
        'refresh-skipped',
        'user-verification-required',
        'user-verification-ok',
        'user-verification-failed',
    ]

    def __init__(self, context_map: ContextMap):
        self._guid = str(uuid4())
        self._events = EventSource(
            [
                'context-sync',
                'auth-disabled',
            ]
            + self.__propagated_auth_event_types,
            origin=self
        )
        self._contexts: ContextMap = context_map

    @property
    def guid(self):
        return self._guid

    @property
    def events(self):
        return self._events

    @property
    def contexts(self):
        return self._contexts

    @classmethod
    def _get_hostname(cls, hostname: str) -> str:
        base_url = hostname if cls._re_http_scheme.search(hostname) else f'https://{hostname}'
        return urlparse(base_url).netloc

    def add(self, context_name: str):
        self._contexts.set(context_name, Context())

    def remove(self, context_name: str):
        self._contexts.unset(context_name)

    def rename(self, old_name: str, new_name: str):
        self._contexts.rename(old_name, new_name)

    def list(self) -> List[ContextMetadata]:
        return self._contexts.list()

    def use(self,
            registry_hostname_or_url: str,
            context_name: Optional[str] = None,
            no_auth: Optional[bool] = False) -> EndpointRepository:
        target_hostname = self._get_hostname(registry_hostname_or_url)
        context_name = context_name or target_hostname

        if context_name is None:
            raise RuntimeError('The name of the context cannot be NULL.')

        context_logger = get_logger(f'{self._logger.name}/{context_name}')
        context_logger.debug(f'Begin the sync procedure (given: {registry_hostname_or_url})')

        context = self._contexts.get(context_name)
        has_context_before = context is not None

        if not has_context_before:
            exact_url_requested = self._re_http_scheme.search(registry_hostname_or_url)

            if exact_url_requested:
                registry_url = self._check_if_root_url_and_sanitize_url(registry_hostname_or_url)
                if registry_url:
                    registry = ServiceRegistry.make(self._create_registry_endpoint_definition(context_name,
                                                                                              registry_url))
                else:
                    raise InvalidServiceRegistryError(
                        f'The given URL ({registry_hostname_or_url}) is not the root URL of the service registry.')
            else:
                registry = self._scan_for_registry_endpoint(target_hostname)
                if not registry:
                    raise InvalidServiceRegistryError(
                        f'The given hostname ({registry_hostname_or_url}) is not a hostname of the service registry service.')

            context = Context()
            self._contexts.set(context_name, context)

            context.endpoints.append(registry.endpoint)

            self._contexts.set(context_name, context)
        else:
            pass  # NOOP

        # Instantiate the service registry manager for the upcoming sync operation.
        reg_manager = ServiceRegistryManager(context=context)
        reg_manager.events.on('endpoint-sync', self._on_endpoint_sync)

        active_registries = [inspected_endpoint
                             for inspected_endpoint in self._contexts.get(context_name).endpoints
                             if inspected_endpoint.type in ServiceRegistry.get_supported_service_types()]
        reg_manager.in_isolation(len(active_registries) <= 1)

        if len(active_registries) == 0:
            self._logger.warning(f"No service registries are registered for the context {context_name}")

        self._logger.debug(f'Number of endpoints: {len(self._contexts.get(context_name).endpoints)}')
        self._logger.debug(f'Number of active registries: {len(active_registries)}')

        for reg_endpoint in active_registries:
            self._logger.debug(f'Syncing: {reg_endpoint.url}')
            reg_manager.synchronize_endpoints(reg_endpoint.id)

        # Set the current context.
        self._contexts.set_current_context_name(context_name)
        self._contexts.set(context_name, context)

        # Initiate the authentication procedure.
        if no_auth:
            self._logger.debug('AUTH disabled')
            self.events.dispatch('auth-disabled', dict())
        else:
            self._logger.debug('AUTH enabled')
            auth_manager = AuthManager(context=self._contexts.current_context)

            # Set up an event relay.
            for event_type in self.__propagated_auth_event_types:
                self.events.relay_from(auth_manager.events, event_type)

            auth_manager.initiate_authentications()
            del auth_manager

        # Then, return the repository.
        return EndpointRepository(self._contexts.get(context_name).endpoints, cacheable=True)

    def _on_endpoint_sync(self, event: Event):
        self.events.dispatch('context-sync', event)

    @classmethod
    def _scan_for_registry_endpoint(cls, hostname: str) -> Optional[ServiceRegistry]:
        """ Scan the service for the list of service info. """
        base_url = hostname if cls._re_http_scheme.search(hostname) else f'https://{hostname}'
        context_name = urlparse(base_url).netloc

        # Base-registry-URL-to-listing-URL map
        potential_registry_base_paths = [
            # This is for a service which implements the service registry at root.
            '',

            # This is for a collection service.
            'service-registry/',

            # This is for an explorer service, e.g., viral.ai.
            'api/service-registry/',
        ]

        for api_path in potential_registry_base_paths:
            registry_url = cls._check_if_root_url_and_sanitize_url(urljoin(base_url, api_path))

            if registry_url:
                return ServiceRegistry.make(cls._create_registry_endpoint_definition(context_name, registry_url))
            else:
                continue

        return None

    @staticmethod
    def _create_registry_endpoint_definition(id: str, url: str) -> ServiceEndpoint:
        return ServiceEndpoint(id=id, url=url, type=STANDARD_SERVICE_REGISTRY_TYPE_V1_0)

    @classmethod
    def _check_if_root_url_and_sanitize_url(cls, registry_url: str):
        root_url = registry_url + ('' if registry_url.endswith('/') else '/')
        listing_url = urljoin(root_url, 'services')

        try:
            response = requests.get(listing_url, headers={'Accept': 'application/json'})
        except requests.exceptions.ConnectionError:
            return None

        if response.ok:
            # noinspection PyBroadException
            try:
                ids = sorted([entry['id'] for entry in response.json()])
                cls._logger.debug(f'CHECK: IDS => {", ".join(ids)}')
            except Exception as e:
                # Look for the next one.
                error_type_name = f'{type(e).__module__}.{type(e).__name__}'
                cls._logger.debug(f'Received OK but failed to parse the response due to ({error_type_name}) {e}')
                cls._logger.debug(f'Here is the response:\n{response.text}')
                return None

            return root_url if response.headers['Content-Type'] == 'application/json' else None
        else:
            return None
        # end: if


@service.registered()
class InMemoryContextManager(BaseContextManager):
    """ Context Manager

        This is designed to use with the CLI package.
    """
    _logger = get_logger('InMemoryContextManager')

    def __init__(self, context_map: InMemoryContextMap):
        super().__init__(context_map)


@service.registered()
class ContextManager(BaseContextManager):
    """ Context Manager

        This is designed to use with the CLI package.
    """
    _logger = get_logger('ContextManager')

    def __init__(self, context_map: ConfigurationBasedContextMap):
        super().__init__(context_map)
