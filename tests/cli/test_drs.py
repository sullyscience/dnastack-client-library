import os
from typing import Optional, Dict, List, Any

from dnastack import use
from dnastack.client.factory import EndpointRepository
from .base import PublisherCliTestCase


class TestDrsCommand(PublisherCliTestCase):
    # Test-specified
    sample_size = 10

    tmp_path = os.path.join(os.getcwd(), 'test-tmp')
    drs_uris = []

    primary_factory: Optional[EndpointRepository] = None
    collection_blob_items_map: Dict[str, List[Dict[str, Any]]] = dict()

    def setUp(self):
        super().setUp()

        # Set up the temporary directory.
        self.execute(f'mkdir -p {self.tmp_path}')
        self.after_this_test(self._clear_temp_files)

        self.input_file_path = os.path.join(self.tmp_path, 'object_list.txt')

        if not self.primary_factory:
            self.primary_factory = use(self.explorer_urls[1], no_auth=True) \
                if self._test_via_explorer \
                else self.get_factory()
            self.set_default_event_interceptors_for_factory(self.primary_factory)

        if not self.collection_blob_items_map:
            self.collection_blob_items_map.update(self._get_collection_blob_items_map(self.primary_factory,
                                                                                      self.sample_size))

        if not self.drs_uris:
            for items in self.collection_blob_items_map.values():
                for item in items:
                    self.drs_uris.append(item.get('metadata_url'))

        self.invoke('use', self.explorer_urls[1] if self._test_via_explorer else self.explorer_urls[0])

    def test_download_files_with_cli_arguments(self):
        self.retry_if_fail(self._test_download_files_with_cli_arguments,
                           intermediate_cleanup=lambda: self._clear_temp_files())

    def _test_download_files_with_cli_arguments(self):
        result = self.invoke('files', 'download', '-o', self.tmp_path, *self.drs_uris)
        self.assertEqual(0, result.exit_code)

        file_name_list = [f for f in os.listdir(self.tmp_path) if f != os.path.basename(self.input_file_path)]
        self.assertGreaterEqual(len(self.drs_uris), len(file_name_list))

        for file_name in file_name_list:
            file_path = os.path.join(self.tmp_path, file_name)
            self.assertTrue(os.path.getsize(file_path) > 0, f'The downloaded {file_path} must not be empty.')

    def test_download_files_with_input_file(self):
        self.retry_if_fail(self._test_download_files_with_input_file,
                           intermediate_cleanup=lambda: self._clear_temp_files(),
                           max_run_count=0)

    def _test_download_files_with_input_file(self):
        # Prepare the input file.
        with open(self.input_file_path, 'w') as f:
            f.write('\n'.join(self.drs_uris))

        result = self.invoke('drs', 'download', '-i', self.input_file_path, '-o', self.tmp_path)
        self.assertEqual(0, result.exit_code)

        file_name_list = [f for f in os.listdir(self.tmp_path) if f != os.path.basename(self.input_file_path)]

        self._logger.debug(f'file_name_list => {file_name_list}')
        self._logger.debug(f'self.drs_urls => {self.drs_uris}')

        self.assertGreaterEqual(len(self.drs_uris), len(file_name_list))

        for file_name in file_name_list:
            file_path = os.path.join(self.tmp_path, file_name)
            self.assertTrue(os.path.getsize(file_path) > 0, f'The downloaded {file_path} must not be empty.')

    def _clear_temp_files(self):
        self.execute(f'rm -rf {self.tmp_path}/*')
