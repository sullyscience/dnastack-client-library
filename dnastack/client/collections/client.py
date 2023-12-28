from typing import List, Union, Optional
from urllib.parse import urljoin

from dnastack.client.base_client import BaseServiceClient
from dnastack.client.collections.model import Collection
from dnastack.client.data_connect import DATA_CONNECT_TYPE_V1_0
from dnastack.client.models import ServiceEndpoint
from dnastack.client.service_registry.models import ServiceType
from dnastack.common.tracing import Span
# Feature: Support the service registry integration
# Feature: Using both root and "singular" soon-to-be-deprecated per-collection data connect endpoints
from dnastack.http.session import ClientError

STANDARD_COLLECTION_SERVICE_TYPE_V1_0 = ServiceType(group='com.dnastack',
                                                    artifact='collection-service',
                                                    version='1.0.0')

# Feature: No support for service registry integration
# Feature: Only using "plural" per-collection data connect endpoint
EXPLORER_COLLECTION_SERVICE_TYPE_V1_0 = ServiceType(group='com.dnastack.explorer',
                                                    artifact='collection-service',
                                                    version='1.0.0')


class InvalidApiResponse(RuntimeError):
    pass


class UnknownCollectionError(RuntimeError):
    def __init__(self, id_or_slug_name, trace: Span):
        super().__init__(id_or_slug_name)
        self.trace = trace


class CollectionServiceClient(BaseServiceClient):
    """Client for Collection API"""

    @staticmethod
    def get_adapter_type() -> str:
        return 'collections'

    @staticmethod
    def get_supported_service_types() -> List[ServiceType]:
        return [
            EXPLORER_COLLECTION_SERVICE_TYPE_V1_0,
            STANDARD_COLLECTION_SERVICE_TYPE_V1_0,
        ]

    def _get_single_collection_url(self, id_or_slug_name: str, extended_path: str = ''):
        return urljoin(self.url, f'collection/{id_or_slug_name}{extended_path}')

    def _get_resource_url(self, id_or_slug_name: str, short_service_type: str):
        return self._get_single_collection_url(id_or_slug_name, f'/{short_service_type}')

    def get(self, id_or_slug_name: str, no_auth: bool = False, trace: Optional[Span] = None) -> Collection:
        """ Get a collection by ID or slug name """
        trace = trace or Span(origin=self)
        local_logger = trace.create_span_logger(self._logger)
        with self.create_http_session(no_auth=no_auth) as session:
            try:
                get_url = self._get_single_collection_url(id_or_slug_name)
                get_response = session.get(get_url, trace_context=trace)
                try:
                    return Collection(**get_response.json())
                except Exception as e:
                    local_logger.error(f'The response from {get_url} is not a JSON string.')
                    local_logger.error(f'\nHTTP {get_response.status_code} (Content-Type: {get_response.headers.get("Content-Type")})\n\n{get_response.text}\n')
                    raise InvalidApiResponse() from e
            except ClientError as e:
                if e.response.status_code == 404:
                    raise UnknownCollectionError(id_or_slug_name, trace) from e
                raise e

    def list_collections(self, no_auth: bool = False, trace: Optional[Span] = None) -> List[Collection]:
        """ List all available collections """
        trace = trace or Span(origin=self)
        with self.create_http_session(no_auth=no_auth) as session:
            res = session.get(urljoin(self.url, 'collections'), trace_context=trace)
            return [Collection(**raw_collection) for raw_collection in res.json()]

    def data_connect_endpoint(self,
                              collection: Union[str, Collection, None] = None,
                              no_auth: bool = False) -> ServiceEndpoint:
        """
        Get the URL to the corresponding Data Connect endpoint

        :param collection: The collection or collection ID. It is optional and only used by the explorer.
        :param no_auth: Trigger this method without invoking authentication even if it is required.
        """
        sub_endpoint = self._endpoint.copy(deep=True)
        sub_endpoint.type = DATA_CONNECT_TYPE_V1_0

        if self._endpoint.dnastack_schema_version == 2.0 and self._get_service_type() == STANDARD_COLLECTION_SERVICE_TYPE_V1_0:
            sub_endpoint.url = urljoin(self._endpoint.url, '/data-connect/')
        else:
            # noinspection PyUnusedLocal
            collection_id = None

            if isinstance(collection, Collection):
                collection_id = collection.slugName
            elif isinstance(collection, str):
                collection_id = collection
            else:
                raise AssertionError(f'For collection/{self._endpoint.dnastack_schema_version} ({self._endpoint.type}), the '
                                     f'given collection must be either an instance of Collection or the ID/slug name '
                                     f'of the collection (string). The given type of collection is '
                                     f'{type(collection).__name__}.')

            # While this part is not really necessary, it is designed as sanity check to ensure that the requested
            # collection exists before providing the data-connect endpoint for the given collection.
            existing_collection = self.get(collection_id, no_auth=no_auth)
            sub_endpoint.url = self._get_single_collection_url(existing_collection.slugName, '/data-connect/')

        if not no_auth and sub_endpoint.authentication:
            auth_type = sub_endpoint.authentication.get('type')

            # Override the resource URL
            if not auth_type or auth_type == 'oauth2':
                # NOTE: Generally, we want to restrict the access only to tables within the scope of the requested
                #       collection. However, due to the recent requirements where the client needs to have access to
                #       "/data-connect/table/system/metadata/catalogs" and the upcoming deprecation of per-collection
                #       data connect controller, the client code will now ask for authorization for the whole service.
                sub_endpoint.authentication['resource_url'] = (
                        self._endpoint.authentication.get('resource_url')
                        or sub_endpoint.url
                )

                # Reset the scope.
                if 'scope' in sub_endpoint.authentication:
                    del sub_endpoint.authentication['scope']

        return sub_endpoint

    def _get_service_type(self) -> ServiceType:
        return self._endpoint.type or self.get_supported_service_types()[0]
