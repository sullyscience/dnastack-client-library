from typing import Union, Optional, Any, Dict

from dnastack.client.base_exceptions import AmbiguousArgumentsError
from dnastack.client.collections.client import CollectionServiceClient as StandardCollectionServiceClient, \
    STANDARD_COLLECTION_SERVICE_TYPE_V1_0
from dnastack.client.collections.model import Collection, COLLECTION_READ_ONLY_PROPERTIES
from dnastack.common.tracing import Span
from dnastack.http.session import JsonPatch


class NoUpdateError(RuntimeError):
    pass


class CollectionServiceClient(StandardCollectionServiceClient):
    # noinspection PyShadowingBuiltins
    def patch(self,
              collection: Optional[Collection] = None,
              trace: Optional[Span] = None,
              *,
              id: Optional[str] = None,
              **attrs) -> Collection:
        assert self.endpoint.type == STANDARD_COLLECTION_SERVICE_TYPE_V1_0, \
            f'The method does not support the endpoint of type {self.endpoint.type}. Please check your configuration.'

        trace = trace or Span(origin=self)

        if id is not None:
            if isinstance(collection, Collection):
                raise AmbiguousArgumentsError('Only require either a collection ID OR object, but not both, at the '
                                              'same time.')
            if not attrs:
                raise AssertionError('No attributes must be defined.')
        else:
            if attrs:
                raise AmbiguousArgumentsError('The overriding attributes must not be defined.')

        collection_id = collection.id if collection else id

        given_overriding_properties = (collection.dict() if collection else (attrs or dict()))
        update_patches = [
            JsonPatch(op='replace', path=f'/{k}', value=v).dict()
            for k, v in given_overriding_properties.items()
            if k not in COLLECTION_READ_ONLY_PROPERTIES and v is not None
        ]

        if not update_patches:
            raise NoUpdateError(f'No updates are required at the moment. It is possible that you are attempting to '
                                f'override read-only properties, such as {", ".join(COLLECTION_READ_ONLY_PROPERTIES)}.')

        with self.create_http_session() as session:
            resource_url = self._get_single_collection_url(collection_id)
            get_response = session.get(resource_url, trace_context=trace)

            # trace_logger = trace.create_span_logger(self._logger)
            assert get_response.status_code == 200, 'Unexpected Response'

            etag = (get_response.headers.get('etag') or '').replace('"', '')
            assert etag, f'GET {resource_url} does not provide ETag. Unable to update the collection.'

            patch_response = session.json_patch(resource_url,
                                                trace_context=trace,
                                                headers={'If-Match': etag},
                                                json=update_patches)

            return Collection(**patch_response.json())


