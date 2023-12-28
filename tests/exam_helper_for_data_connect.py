import logging
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from os import cpu_count
from threading import Lock, Semaphore
from typing import List, Optional

from dnastack import DataConnectClient, ServiceEndpoint
from dnastack.client.data_connect import TableInfo
from dnastack.client.factory import EndpointRepository
from dnastack.common.logger import get_logger


class DataConnectTestCaseMixin:
    _max_approx_usable_table_count = 4
    _usable_table_names: Optional[List[str]] = None

    _table_scanning_network_lock = Semaphore(_max_approx_usable_table_count)
    _table_scanning_sync_lock = Lock()

    @property
    def usable_table_names(self) -> List[str]:
        if self._usable_table_names is None:
            self._scan_for_usable_tables()
        return self._usable_table_names

    @classmethod
    def _scan_for_usable_tables(cls):
        logger = get_logger(f'DataConnectTestCaseMixin/table-scanner', logging.INFO)
        logger.info('Scanning for usable tables')

        worker_count = min(2, cpu_count() * 2, cls._max_approx_usable_table_count)

        cls._usable_table_names = []

        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            for endpoint in cls._get_data_connect_endpoints():
                client = DataConnectClient.make(endpoint)

                futures: List[Future] = list()

                for target_table in client.list_tables():
                    futures.append(
                        pool.submit(cls._check_if_table_is_usable,
                                    client=client,
                                    target_table=target_table)
                    )

            cls._usable_table_names.extend([
                future.result()
                for future in as_completed(futures)
                if future.result() is not None
            ])

            if not cls._usable_table_names:
                raise RuntimeError('No usable tables for any Data Connect tests')

            logger.info('Scanning for usable tables is complete.')

    @classmethod
    def _check_if_table_is_usable(cls, client: DataConnectClient, target_table: TableInfo):
        max_approx_usable_table_count = cls._max_approx_usable_table_count

        with cls._table_scanning_network_lock:
            logger = get_logger(f'DataConnectTestCaseMixin/table-checker')
            table = client.table(target_table)

            with cls._table_scanning_sync_lock:
                if len(cls._usable_table_names) >= max_approx_usable_table_count:
                    return 2  # excluded due to limit

            try:
                table_info = table.info
            except Exception as e:
                logger.info(f'T/{table.name}: Failed to check the info ({type(e).__name__}: {e})')
                return 0  # excluded due to error

            try:
                column_name = list(table_info.data_model.get("properties").keys())[0]
                __ = [row for row in client.query(f'SELECT {column_name} FROM {table.name} LIMIT 1')]
            except Exception as e:
                logger.info(f'T/{table.name}: Failed to check the data access ({type(e).__name__}: {e})')
                return 0  # excluded due to error

            with cls._table_scanning_sync_lock:
                if len(cls._usable_table_names) >= max_approx_usable_table_count:
                    return 2  # excluded due to limit
                cls._usable_table_names.append(table.name)

            return 1  # included

    @classmethod
    def _get_data_connect_endpoints(cls) -> List[ServiceEndpoint]:
        # noinspection PyUnresolvedReferences
        factory: EndpointRepository = cls.get_factory()

        return [
            endpoint
            for endpoint in factory.all()
            if endpoint.type in DataConnectClient.get_supported_service_types()
        ]

    @classmethod
    def _get_data_connect_client(cls, index: int = 0) -> Optional[DataConnectClient]:
        compatible_endpoints = cls._get_data_connect_endpoints()

        if not compatible_endpoints:
            raise RuntimeError('No Data Connect-compatible endpoints for this test')

        if index >= len(compatible_endpoints):
            raise RuntimeError(f'Requested Data Connect-compatible endpoint #{index} but it does not exist.')

        compatible_endpoint = compatible_endpoints[index]

        # import pprint
        # pprint.pprint(compatible_endpoint.dict(), indent=2)

        return DataConnectClient.make(compatible_endpoint)
