from typing import List, Iterator, Optional
from urllib.parse import urljoin

from dnastack.alpha.client.workbench.base_client import BaseWorkbenchClient
from dnastack.alpha.client.workflow.models import WorkflowDescriptor, WorkflowListResult, Workflow, WorkflowCreate, \
    WorkflowVersionCreate, WorkflowVersion, WorkflowVersionListResult, WorkflowSource
from dnastack.http.session import JsonPatch
from dnastack.client.models import ServiceEndpoint
from dnastack.client.service_registry.models import ServiceType


class WorkflowClient(BaseWorkbenchClient):

    @staticmethod
    def get_adapter_type() -> str:
        return 'workflow-service'

    @staticmethod
    def get_supported_service_types() -> List[ServiceType]:
        return [
            ServiceType(group='com.dnastack.workbench', artifact='workflow-service', version='1.0.0'),
        ]

    # noinspection PyMethodOverriding
    @classmethod
    def make(cls, endpoint: ServiceEndpoint, namespace: str):
        """Create this class with the given `endpoint` and `namespace`."""
        if not endpoint.type:
            endpoint.type = cls.get_default_service_type()
        return cls(endpoint, namespace)

    def get_json_schema(self, workflow_id: str, version_id: str) -> WorkflowDescriptor:
        with self.create_http_session() as session:
            response = session.get(
                urljoin(self.endpoint.url, f'{self.namespace}/workflows/{workflow_id}/versions/{version_id}/describe'))
        return WorkflowDescriptor(**response.json())

    def list_workflows(self, source: Optional[WorkflowSource], include_deleted: Optional[bool] = False) -> Iterator[
        Workflow]:
        with self.create_http_session() as session:
            response = session.get(
                urljoin(self.endpoint.url, f'{self.namespace}/workflows?deleted={include_deleted}')
            )
            workflows = WorkflowListResult(**response.json()).workflows
            if source:
                workflows = [w for w in workflows if w.source == source]
        return iter(workflows)

    def list_workflow_versions(self, workflow_id: str, include_deleted: Optional[bool] = False) -> Iterator[
        WorkflowVersion]:
        with self.create_http_session() as session:
            response = session.get(
                urljoin(self.endpoint.url,
                        f'{self.namespace}/workflows/{workflow_id}/versions?deleted={include_deleted}'))
        return iter(WorkflowVersionListResult(**response.json()).versions)

    def get_workflow(self, workflow_id: str, include_deleted: Optional[bool] = False) -> Workflow:
        with self.create_http_session() as session:
            response = session.get(
                urljoin(self.endpoint.url, f'{self.namespace}/workflows/{workflow_id}?deleted={include_deleted}'))
            workflow = Workflow(**response.json())
            workflow.etag = response.headers.get("Etag").strip("\"")
            return workflow

    def get_workflow_version(self, workflow_id: str, version_id: str,
                             include_deleted: Optional[bool] = False) -> WorkflowVersion:
        with self.create_http_session() as session:
            response = session.get(
                urljoin(self.endpoint.url,
                        f'{self.namespace}/workflows/{workflow_id}/versions/{version_id}?deleted={include_deleted}'))
            workflow_version = WorkflowVersion(**response.json())
            workflow_version.etag = response.headers.get("Etag").strip("\"")
            return workflow_version

    def create_workflow(self, workflow_create_request: WorkflowCreate) -> Workflow:
        with self.create_http_session() as session:
            response = session.post(
                urljoin(self.endpoint.url, f'{self.namespace}/workflows'), json=workflow_create_request.dict())
        return Workflow(**response.json())

    def create_version(self, workflow_id: str,
                       workflow_version_create_request: WorkflowVersionCreate) -> WorkflowVersion:
        with self.create_http_session() as session:
            response = session.post(
                urljoin(self.endpoint.url, f'{self.namespace}/workflows/{workflow_id}/versions'),
                json=workflow_version_create_request.dict())
        return WorkflowVersion(**response.json())

    def delete_workflow(self, workflow_id: str, etag: str):
        with self.create_http_session() as session:
            session.delete(
                urljoin(self.endpoint.url, f'{self.namespace}/workflows/{workflow_id}'),
                headers={'If-Match': etag}
            )

    def delete_workflow_version(self, workflow_id: str, version_id: str, etag: str):
        with self.create_http_session() as session:
            session.delete(
                urljoin(self.endpoint.url, f'{self.namespace}/workflows/{workflow_id}/versions/{version_id}'),
                headers={'If-Match': etag}
            )

    def update_workflow(self, workflow_id: str, etag: str, updates: List[JsonPatch]) -> Workflow:
        with self.create_http_session() as session:
            updates = [update.dict() for update in updates]
            response = session.json_patch(
                urljoin(self.endpoint.url, f'{self.namespace}/workflows/{workflow_id}'),
                headers={'If-Match': etag},
                json=updates
            )
            return Workflow(**response.json())

    def update_workflow_version(self, workflow_id: str, version_id: str, etag: str,
                                updates: List[JsonPatch]) -> WorkflowVersion:
        with self.create_http_session() as session:
            updates = [update.dict() for update in updates]
            response = session.json_patch(
                urljoin(self.endpoint.url, f'{self.namespace}/workflows/{workflow_id}/versions/{version_id}'),
                headers={'If-Match': etag},
                json=updates
            )
            return WorkflowVersion(**response.json())
