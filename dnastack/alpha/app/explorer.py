from abc import ABC
from copy import deepcopy
from datetime import datetime
from enum import Enum
from typing import List, Optional, Any, Dict, Iterator, Iterable, Callable

from pydantic import BaseModel, Field

from dnastack.client.collections.client import CollectionServiceClient, UnknownCollectionError
from dnastack.client.collections.model import Collection as CollectionModel
from dnastack.client.data_connect import DataConnectClient
from dnastack.client.drs import DrsClient, Blob
from dnastack.client.factory import EndpointRepository
from dnastack.client.models import ServiceEndpoint
from dnastack.common.exceptions import DependencyError
from dnastack.common.logger import get_logger_for
from dnastack.common.simple_stream import SimpleStream
from dnastack.context.helper import use


class NoCollectionError(RuntimeError):
    pass


class TooManyCollectionsError(RuntimeError):
    pass


class _SearchOperation:
    def __init__(self, dc: DataConnectClient, no_auth: bool, query: str):
        self._dc = dc
        self._no_auth = no_auth
        self.__query = query

    def load_data(self) -> Iterator[Dict[str, Any]]:
        return self._dc.query(self.__query, no_auth=self._no_auth)

    def to_list(self) -> List[Dict[str, Any]]:
        return [row for row in self.load_data()]

    def to_data_frame(self):
        try:
            # We delay the import as late as possible so that the optional dependency (pandas)
            # does not block the other functionalities of the library.
            import pandas as pd
            return pd.DataFrame(self.load_data())
        except ImportError:
            raise DependencyError('pandas')


class BasicSimplifiedLibraryItem(BaseModel, ABC):
    """ Base Simplified Library Item

        Based on https://github.com/DNAstack/indexing-service/blob/main/src/main/java/com/dnastack/indexingservice/library/LibraryItem.java.
    """
    id: str
    name: str
    type: str
    size: int
    size_unit: str
    item_updated_time: datetime


class SimplifiedTableMetadata(BasicSimplifiedLibraryItem):
    """ Simplified Library Item """
    json_schema: Dict[str, Any]


class SimplifiedBlobMetadata(BasicSimplifiedLibraryItem):
    """ Simplified Library Item """
    checksums: List[Dict[str, str]] = Field(default_factory=list)


class ItemType(Enum):
    BLOB = 'blob'
    TABLE = 'table'


class Collection:
    """ High-level Collection API Client """

    def __init__(self,
                 factory: EndpointRepository,
                 cs: CollectionServiceClient,
                 collection: CollectionModel,
                 no_auth: bool):
        self._logger = get_logger_for(self)
        self._factory = factory
        self._cs = cs
        self._collection = collection
        self._no_auth = no_auth
        self._dc: Optional[DataConnectClient] = None
        self._drs: DrsClient = self._factory.get_one_of(client_class=DrsClient)

    def get_record(self) -> CollectionModel:
        return self._collection

    def query(self, query: str):
        return _SearchOperation(self.data_connect(), self._no_auth, query)

    def list_items(self,
                   *,
                   limit: Optional[int],
                   kind: Optional[ItemType] = None,
                   kinds: Optional[Iterable[ItemType]] = None,
                   on_has_more_result: Optional[Callable[[int], None]] = None) -> List[BasicSimplifiedLibraryItem]:
        # We opt for an enum on item types (kind/kinds) in this case to avoid SQL-injection attempts.
        assert limit >= 0, 'The limit has to be ZERO (no limit) or at least 1 (to impose the limit).'

        items: List[BasicSimplifiedLibraryItem] = []

        items_query = self._collection.itemsQuery.strip()

        # We use +1 as an indicator whether there are more results.
        actual_items_query = f'SELECT * FROM ({items_query})'

        if kind:
            actual_items_query = f"{actual_items_query} WHERE type = '{kind.value}'"

        if kinds:
            types = ', '.join([f"'{kind}'" for kind in kinds])
            actual_items_query = f"{actual_items_query} WHERE type IN {types}"

        if limit is not None and limit > 1:
            actual_items_query = f"{actual_items_query} LIMIT {limit + 1}"

        items.extend([
            self.__simplify_item(i)
            for i in self.data_connect().query(actual_items_query, no_auth=self._no_auth)
        ])

        row_count = len(items)

        if 0 < limit < row_count and on_has_more_result and callable(on_has_more_result):
            on_has_more_result(row_count)

        return items

    def data_connect(self):
        if not self._dc:
            default_no_auth_properties = {'authentication': None, 'fallback_authentications': None}

            proposed_data_connect_endpoint = self._cs.data_connect_endpoint(self._collection.slugName,
                                                                            no_auth=self._no_auth)

            target_endpoint: Optional[ServiceEndpoint] = None

            # Look up for any similar registered service endpoint.
            for endpoint in self._factory.all(client_class=DataConnectClient):
                proposed_data_connect_endpoint_url = proposed_data_connect_endpoint.url
                if not proposed_data_connect_endpoint_url.endswith('/'):
                    proposed_data_connect_endpoint_url += '/'

                reference_data_connect_endpoint_url = endpoint.url
                if not reference_data_connect_endpoint_url.endswith('/'):
                    reference_data_connect_endpoint_url += '/'

                if proposed_data_connect_endpoint_url == reference_data_connect_endpoint_url:
                    target_endpoint = endpoint
                    break

            if not target_endpoint:
                target_endpoint = proposed_data_connect_endpoint

                self._logger.debug(
                    f'Unable to find a registered {proposed_data_connect_endpoint.type} endpoint '
                    f'at {proposed_data_connect_endpoint.url}.'
                )

            self._dc = DataConnectClient.make(
                target_endpoint.copy(update=default_no_auth_properties)
                if self._no_auth
                else target_endpoint
            )

        return self._dc

    def blob(self, *, id: Optional[str] = None, name: Optional[str] = None) -> Optional[Blob]:
        blobs = self.blobs(ids=[id] if id else [], names=[name] if name else [])
        if blobs:
            return blobs.get(id if id is not None else name)
        else:
            return None

    def blobs(self, *, ids: Optional[List[str]] = None, names: Optional[List[str]] = None) -> Dict[str, Optional[Blob]]:
        assert ids or names, 'One of the arguments MUST be defined.'

        if ids:
            conditions: str = ' OR '.join([
                f"(id = '{id}')"
                for id in ids
            ])
        elif names:
            conditions: str = ' OR '.join([
                f"(name = '{name}')"
                for name in names
            ])
        else:
            raise NotImplementedError()

        id_to_name_map: Dict[str, str] = SimpleStream(
            self.query(f"SELECT id, name FROM ({self._collection.itemsQuery}) WHERE {conditions}").load_data()
        ).to_map(lambda row: row['id'], lambda row: row['name'])

        return {
            id if ids is not None else id_to_name_map[id]: self._drs.get_blob(id)
            for id in id_to_name_map.keys()
        }

    def find_blob_by_name(self, objectname: str, catalog_name: Optional[str] = "collections", table_name: Optional[str] = "_files", column_name: Optional[str] = "drs_url") -> Blob:
        # TODO Unify this method with "blobs(...)"
        # language=sql
        db_slug = self._collection.slugName.replace("-", "_")
        q = f"SELECT {column_name} FROM \"{catalog_name}\".\"{db_slug}\".\"{table_name}\" WHERE name='{objectname}' LIMIT 1"
        results = self.query(q)
        return self._drs.get_blob(next(results.load_data())['drs_url'])

    @staticmethod
    def __simplify_item(row: Dict[str, Any]) -> BasicSimplifiedLibraryItem:
        if row['type'] == ItemType.BLOB.value:
            return SimplifiedBlobMetadata(**row)
        elif row['type'] == ItemType.TABLE.value:
            row_copy = deepcopy(row)
            row_copy['name'] = (
                    row.get('qualified_table_name')
                    or row.get('preferred_name')
                    or row.get('display_name')
                    or row['name']
            )
            return SimplifiedTableMetadata(**row_copy)
        else:
            return BasicSimplifiedLibraryItem(**row)


class Explorer:
    """ High-level Explorer API Client

        .. code-block:: python

            from dnastack.alpha.app.explorer import Explorer

            # Data Connect
            query = collection.query('SELECT * FROM collections.public_datasets.metadata LIMIT 5')
            df = query.to_data_frame()
            rows = query.to_list()

            # DRS (not yet manually tested with collections with blobs)
            blob: Optional[Blob] = collection.blob(id='123-456') or collection.blob(name='foo-bar')
            blobs: Dict[str, Optional[Blob]] = collection.blobs(ids=['123-456']) or collection.blobs(names=['foo-bar'])

    """

    def __init__(self, context_name_or_url: str, *, no_auth: bool = False):
        self._context_name_or_url = context_name_or_url
        self._factory = use(self._context_name_or_url, no_auth=no_auth)
        self._cs: CollectionServiceClient = self._factory.get_one_of(client_class=CollectionServiceClient)
        self._no_auth = no_auth

    def list_collections(self) -> List[CollectionModel]:
        return self._cs.list_collections(no_auth=self._no_auth)

    def collection(self, id_or_slug_name: Optional[str] = None, *, name: Optional[str] = None) -> Collection:
        # NOTE: "ID" and "slug name" are unique identifier whereas "name" is not.
        assert id_or_slug_name or name, 'One of the arguments MUST be defined.'

        if id_or_slug_name is not None:
            try:
                collection = self._cs.get(id_or_slug_name, no_auth=self._no_auth)
            except UnknownCollectionError as e:
                raise NoCollectionError(id_or_slug_name)
            return Collection(self._factory, self._cs, collection, no_auth=self._no_auth)
        elif name is not None:
            assert name.strip(), 'The name cannot be empty.'
            target_collections = SimpleStream(self._cs.list_collections(no_auth=self._no_auth))\
                .filter(lambda endpoint: name == endpoint.name)\
                .to_list()
            if len(target_collections) == 1:
                return Collection(self._factory, self._cs, target_collections[0], no_auth=self._no_auth)
            elif len(target_collections) == 0:
                raise NoCollectionError(name)
            else:
                raise TooManyCollectionsError(target_collections)
        else:
            raise NotImplementedError()
