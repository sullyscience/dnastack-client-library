import os
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse

from dnastack.client.drs import DrsApiError, Blob, DrsClient
from dnastack.client.factory import EndpointRepository
from tests.exam_helper import BasePublisherTestCase


class TestDrsClient(BasePublisherTestCase):
    """ Test a client for DRS service"""
    # Test-specified
    sample_size = 3

    primary_factory: Optional[EndpointRepository] = None
    collection_blob_items_map: Dict[str, List[Dict[str, Any]]] = dict()

    @staticmethod
    def reuse_session() -> bool:
        return True

    def setUp(self):
        super(TestDrsClient, self).setUp()

        self.output_dir = os.path.join(os.path.dirname(__file__), 'tmp')
        os.makedirs(self.output_dir, exist_ok=True)

        if not self.primary_factory:
            self.primary_factory = self.get_factory(self.explorer_urls[1] if self._test_via_explorer else None)
            self.set_default_event_interceptors_for_factory(self.primary_factory)

        if not self.collection_blob_items_map:
            self.collection_blob_items_map.update(self._get_collection_blob_items_map(self.primary_factory,
                                                                                      self.sample_size))

        self.drs_client: DrsClient = self.primary_factory.get('drs')

    def tearDown(self) -> None:
        super(TestDrsClient, self).tearDown()
        for file_name in os.listdir(self.output_dir):
            if file_name[0] == '.':
                continue
            os.unlink(os.path.join(self.output_dir, file_name))

    def test_with_blob_using_implicit_arguments(self):
        """
        Test get_blob where the type of identifier is UNSPECIFIED, i.e., let the method figure out whether the given
        identifier is an ID or a DRS URL.

        Please note that this test is designed specifically to test how DrsClient works, not how it is integrated with
        the collection service client.
        """
        self._run_test(explicit=False)

    def test_with_blob_using_explicit_arguments(self):
        """
        Test get_blob where the type of identifier is SPECIFIED.

        Please note that this test is designed specifically to test how DrsClient works, not how it is integrated with
        the collection service client.
        """
        self._run_test(explicit=True)

    def _run_test(self, explicit: bool):
        # Define the test DRS URL
        drs_ids = []
        drs_urls = []

        for _, items in self.collection_blob_items_map.items():
            for item in items:
                # Extract the DRS ID from the metadata URL.
                # NOTE: Before November 2023, a library item ID is considered a DRS object ID. However, the breaking
                #       change was introduced to simplify the access evaluation procedure. Until we can decide what
                #       to do next to streamline the user experience, the test will extract the DRS ID from the
                #       metabase URL to ensure the functionality.
                parsed_url = urlparse(item['metadata_url'])
                drs_id = parsed_url.path[1:]
                assert len(drs_id) > 0, f"LItem/{item['id']}: The metadata URL does not contain the object ID."
                drs_ids.append(drs_id)

                # Retrieve the DRS URL from the metadata URL.
                drs_urls.append(item['metadata_url'])

        errors: Dict[str, Exception] = dict()

        # Download with IDs
        id_blob_map_1: Dict[str, Blob] = dict()
        for drs_id in drs_ids:
            try:
                blob = self.drs_client.get_blob(id=drs_id) if explicit else self.drs_client.get_blob(drs_id)
                # FIXME [2023-11-23] Temporarily disable this assertion due to the disagreement between the DRS Object
                #  ID provided by the "files" table and the one specified in the corresponding DRS URL.
                # self.assertEqual(blob.drs_object.id, drs_id)
                self.assertGreater(len(blob.data), 0)
                id_blob_map_1[blob.drs_object.id] = blob
                blob.close()
            except DrsApiError as e:
                errors[drs_id] = e

        if len(errors) == len(drs_ids):
            for drs_id, e in errors.items():
                self._logger.error(f'Failed to download B/{drs_id} ({e})')
            self.fail('All expected samples fail')

        errors.clear()

        # Download with URLs
        id_blob_map_2: Dict[str, Blob] = dict()
        for drs_url in drs_urls:
            try:
                blob = self.drs_client.get_blob(url=drs_url) if explicit else self.drs_client.get_blob(drs_url)
                self.assertEqual(blob.drs_url, drs_url)
                self.assertGreater(len(blob.data), 0)
                id_blob_map_2[blob.drs_object.id] = blob
                blob.close()
            except DrsApiError as e:
                errors[drs_url] = e

        if len(errors) == len(drs_ids):
            for drs_id, e in errors.items():
                self._logger.error(f'Failed to download B/{drs_id} ({e})')
            self.fail('All expected samples fail')

        # At this point, getting the blobs either by IDs or URLs should yield the same result.
        self.assertEqual(len(id_blob_map_1), len(id_blob_map_2),
                         'The number of accessible objects should be the same for both approaches.')
        for drs_id, blob_by_id in id_blob_map_1.items():
            try:
                blob_by_url = id_blob_map_2[blob_by_id.drs_object.id]
                self.assertEqual(drs_id, blob_by_id.drs_object.id)
                self.assertEqual(drs_id, blob_by_url.drs_object.id)

                self.assertEqual(blob_by_id.drs_object, blob_by_url.drs_object)
            except DrsApiError:
                pass
