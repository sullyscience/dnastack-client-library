from tests.cli.auth_utils import handle_device_code_flow
from tests.cli.base import PublisherCliTestCase


class TestSmoke(PublisherCliTestCase):
    @staticmethod
    def reuse_session() -> bool:
        return True

    @staticmethod
    def automatically_authenticate() -> bool:
        return True

    def test_happy_path(self):
        self.invoke('use', self.explorer_urls[0])

        for collection in self.simple_invoke('collections', 'list'):
            collection_id = collection['slugName']
            try:
                self.invoke('collections', 'tables', 'list', '--collection', collection_id)
                self.invoke('collections', 'query', '-c', collection_id, 'SELECT 1')
                self.invoke('collections', 'query', '--collection', collection_id, 'SELECT 1')
                return
            except:
                pass

        self.fail('No usable collection for the CLI smoke test')
