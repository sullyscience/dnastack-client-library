from typing import Optional

from imagination import container

from dnastack.alpha.client.workflow.client import WorkflowClient
from dnastack.cli.helpers.client_factory import ConfigurationBasedClientFactory
from dnastack.cli.workbench.utils import _populate_workbench_endpoint

WORKBENCH_HOSTNAME = "workbench.dnastack.com"


def get_workflow_client(context_name: Optional[str] = None,
                        endpoint_id: Optional[str] = None,
                        namespace: Optional[str] = None) -> WorkflowClient:
    factory: ConfigurationBasedClientFactory = container.get(ConfigurationBasedClientFactory)
    try:
        return factory.get(WorkflowClient, endpoint_id=endpoint_id, context_name=context_name, namespace=namespace)
    except AssertionError:
        _populate_workbench_endpoint()
        return factory.get(WorkflowClient, endpoint_id=endpoint_id, context_name=context_name, namespace=namespace)
