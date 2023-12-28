from typing import TypeVar

from dnastack.client.workbench.ewes.client import EWesClient
from dnastack.client.workbench.workflow.client import WorkflowClient
from dnastack.client.base_client import BaseServiceClient
from dnastack.client.collections.client import CollectionServiceClient
from dnastack.client.data_connect import DataConnectClient
from dnastack.client.drs import DrsClient
from dnastack.client.service_registry.client import ServiceRegistry

# All known client classes
ALL_SERVICE_CLIENT_CLASSES = (CollectionServiceClient, DataConnectClient, DrsClient, ServiceRegistry, EWesClient, WorkflowClient)

# All client classes for data access
DATA_SERVICE_CLIENT_CLASSES = (CollectionServiceClient, DataConnectClient, DrsClient, EWesClient, WorkflowClient)

# Type variable for the service client
SERVICE_CLIENT_CLASS = TypeVar('SERVICE_CLIENT_CLASS',
                               BaseServiceClient,
                               EWesClient,
                               WorkflowClient,
                               CollectionServiceClient,
                               DataConnectClient,
                               DrsClient,
                               ServiceRegistry)
