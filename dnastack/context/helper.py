from typing import Optional

from imagination import container

from dnastack.client.factory import EndpointRepository
from dnastack.common.logger import get_logger
from dnastack.context.manager import BaseContextManager, InMemoryContextManager


class UnknownContextError(RuntimeError):
    pass


def use(registry_hostname_or_url: str,
        no_auth: Optional[bool] = False,
        existing_context_manager: Optional[BaseContextManager] = None) -> EndpointRepository:
    """
    Initiate a client factory based on the given hostname, i.e., the name of the context.

    If the context does not exist, it will scan the given host server for the service
    registry API and use the API to import service endpoints from /services.

    The "in_isolation" argument is to prevent the current configuration from
    being created or modified. It is designed to use in the library. When it is
    set to "true", instead of loading the configuration from the configuration
    file, this method will use a dummy/blank configuration object.
    """
    logger = get_logger('use')
    manager: BaseContextManager = existing_context_manager or container.get(InMemoryContextManager)
    factory = manager.use(registry_hostname_or_url, no_auth=no_auth)
    if factory:
        return factory
    else:
        logger.error(f'Failed to switch to "{registry_hostname_or_url}". (Existing: {manager.contexts.all().keys()})')
        raise UnknownContextError(registry_hostname_or_url)
