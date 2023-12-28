from dnastack import ServiceEndpoint
from dnastack.common.simple_stream import SimpleStream
from dnastack.context.models import Context


def get_endpoint_by_id(context: Context, id: str) -> ServiceEndpoint:
    return SimpleStream(context.endpoints) \
        .filter(lambda e: e.id == id) \
        .find_first()
