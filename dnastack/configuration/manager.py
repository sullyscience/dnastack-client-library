import os
import shutil

import yaml
from imagination.decorator import service, EnvironmentVariable
from pydantic import ValidationError

from dnastack.client.collections.client import EXPLORER_COLLECTION_SERVICE_TYPE_V1_0, \
    STANDARD_COLLECTION_SERVICE_TYPE_V1_0, CollectionServiceClient
from dnastack.client.data_connect import DATA_CONNECT_TYPE_V1_0, DataConnectClient
from dnastack.client.drs import DRS_TYPE_V1_1, DrsClient
from dnastack.client.models import ServiceEndpoint
from dnastack.common.logger import get_logger
from dnastack.configuration.models import Configuration, DEFAULT_CONTEXT
from dnastack.constants import LOCAL_STORAGE_DIRECTORY
from dnastack.context.models import Context


class InvalidExistingConfigurationError(RuntimeError):
    pass


@service.registered(
    params=[
        EnvironmentVariable('DNASTACK_CONFIG_FILE', default=os.path.join(LOCAL_STORAGE_DIRECTORY, 'config.yaml'),
                            allow_default=True)
    ]
)
class ConfigurationManager:
    def __init__(self, file_path: str):
        self.__logger = get_logger(f'{type(self).__name__}')
        self.__file_path = file_path
        self.__swap_file_path = f'{self.__file_path}.swp'

    def hard_reset(self):
        if os.path.exists(self.__file_path):
            self.__logger.warning('Resetting the configuration')
            os.unlink(self.__file_path)
            self.__logger.warning('Successfully reset the configuration')
        else:
            self.__logger.warning('No configuration to reset')

    def load_raw(self) -> str:
        """ Load the raw configuration content """
        if not os.path.exists(self.__file_path):
            return '{}'
        with open(self.__file_path, 'r') as f:
            return f.read()

    def load(self) -> Configuration:
        """ Load the configuration object """
        self.__logger.debug(f'Reading the configuration from {self.__file_path}...')
        raw_config = self.load_raw()
        if not raw_config:
            return Configuration()
        try:
            config = Configuration(**yaml.load(raw_config, Loader=yaml.SafeLoader))
            return self.migrate(config)
        except ValidationError as e:
            raise InvalidExistingConfigurationError(f'The existing configuration file at {self.__file_path} is invalid.') from e

    def save(self, configuration: Configuration):
        """ Save the configuration object """
        # Note (1): This is designed to have file operation done as quickly as possible to reduce race conditions.
        # Note (2): Instead of interfering with the main file directly, the new content is written to a temp file before
        #           swapping with the real file to minimize the I/O block.
        self.__logger.debug(f'Saving the configuration to {self.__file_path}...')
        configuration = self.migrate(configuration)

        # Perform sanity checks
        for context_name, context in configuration.contexts.items():
            duplicate_endpoint_id_count_map = dict()
            for endpoint in context.endpoints:
                if endpoint.id not in duplicate_endpoint_id_count_map:
                    duplicate_endpoint_id_count_map[endpoint.id] = 0
                duplicate_endpoint_id_count_map[endpoint.id] += 1
            duplicate_endpoint_ids = sorted([id for id, count in duplicate_endpoint_id_count_map.items() if count > 1])
            assert len(duplicate_endpoint_ids) == 0, \
                f'Detected at least two endpoints with the same ID ({", ".join(duplicate_endpoint_ids)}) '\
                f'in the "{context_name}" context'

        # Save the changes.
        new_content = yaml.dump(configuration.dict(exclude_none=True), Dumper=yaml.SafeDumper)
        if not os.path.exists(os.path.dirname(self.__swap_file_path)):
            os.makedirs(os.path.dirname(self.__swap_file_path), exist_ok=True)
        with open(self.__swap_file_path, 'w') as f:
            f.write(new_content)
        shutil.copyfile(self.__swap_file_path, self.__file_path)
        os.unlink(self.__swap_file_path)

    @classmethod
    def migrate(cls, configuration: Configuration) -> Configuration:
        """
        Perform on-line migration on the Configuration object.
        """
        if configuration.version == 3:
            for endpoint in configuration.endpoints:
                cls.migrate_endpoint(endpoint)

            default_context = Context(defaults=configuration.defaults,
                                      endpoints=configuration.endpoints)

            configuration.version = 4
            configuration.contexts[DEFAULT_CONTEXT] = default_context
            configuration.current_context = DEFAULT_CONTEXT
            configuration.defaults = None
            configuration.endpoints = None
        elif configuration.version == 4:
            for context in configuration.contexts.values():
                for endpoint in context.endpoints:
                    cls.migrate_endpoint(endpoint)
            configuration.defaults = None
            configuration.endpoints = None
        else:
            raise UnsupportedModelVersionError(f'{type(configuration).__name__}/{configuration.version}')

        return configuration

    @classmethod
    def migrate_endpoint(cls, endpoint: ServiceEndpoint) -> ServiceEndpoint:
        """
        Perform on-line migration on the ServiceEndpoint object.
        """
        if endpoint.dnastack_schema_version is None or endpoint.dnastack_schema_version == 1.0:
            endpoint.dnastack_schema_version = 2.0

            if not endpoint.type:
                if endpoint.adapter_type == CollectionServiceClient.get_adapter_type():
                    endpoint.type = (
                        EXPLORER_COLLECTION_SERVICE_TYPE_V1_0
                        if endpoint.mode == 'explorer'
                        else STANDARD_COLLECTION_SERVICE_TYPE_V1_0
                    )
                elif endpoint.adapter_type == DataConnectClient.get_adapter_type():
                    endpoint.type = DATA_CONNECT_TYPE_V1_0
                elif endpoint.adapter_type == DrsClient.get_adapter_type():
                    endpoint.type = DRS_TYPE_V1_1
                else:
                    raise NotImplementedError(f'This type of endpoint ({endpoint.adapter_type}) is not supported.')

                endpoint.adapter_type = None
                endpoint.mode = None

        return endpoint


class UnsupportedModelVersionError(RuntimeError):
    pass
