from typing import Set

from .base import PublisherCliTestCase


class TestConfiguration(PublisherCliTestCase):
    def test_list_available_properties(self):
        result = self.simple_invoke('config', 'schema')
        self.assertIn('description', result)
        self.assertIsNotNone(result.get('properties'))
        self.assertEqual(result.get('type'), 'object')
