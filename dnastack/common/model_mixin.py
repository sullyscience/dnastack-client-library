import hashlib

import json


class JsonModelMixin:
    def get_content_hash(self):
        # noinspection PyUnresolvedReferences
        return self.hash(self.dict(exclude_none=True))

    @classmethod
    def hash(self, content):
        raw_config = json.dumps(content, sort_keys=True)
        h = hashlib.new('sha256')
        h.update(raw_config.encode('utf-8'))
        return h.hexdigest()