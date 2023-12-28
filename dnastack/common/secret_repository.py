import sys
from typing import List

# These are optional requirements.
from google.api_core.exceptions import NotFound

try:
    from google.cloud.secretmanager_v1 import SecretManagerServiceClient, SecretVersion, Secret
except ImportError:
    raise RuntimeError('Please install "google-cloud-secret-manager" and try again.')

try:
    import google_crc32c
except ImportError:
    raise RuntimeError('Please install "google-crc32c" and try again.')


class SecretNotFound(RuntimeError):
    pass


class NoSecretVersionAvailable(RuntimeError):
    pass


class SecretRepository:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = SecretManagerServiceClient()

    def create(self, secret_id: str) -> Secret:
        parent_id = f'projects/{self.project_id}'
        return self.client.create_secret(request=dict(parent=parent_id,
                                               secret_id=secret_id,
                                               secret=dict(replication=dict(automatic=dict()))))

    def create_if_not_exist(self, secret_id: str) -> List[SecretVersion]:
        try:
            return self.list_versions(secret_id)
        except SecretNotFound:
            self.create(secret_id)
            return []

    def set(self, secret_id: str, payload: str) -> SecretVersion:
        payload_in_bytes = payload.encode("UTF-8")

        parent_id = self.client.secret_path(self.project_id, secret_id)

        # Calculate payload checksum. Passing a checksum in add-version request
        # is optional.
        crc32c = google_crc32c.Checksum()
        crc32c.update(payload_in_bytes)

        # Add the secret version.
        response = self.client.add_secret_version(
            request={
                "parent": parent_id,
                "payload": {"data": payload_in_bytes,
                            "data_crc32c": int(crc32c.hexdigest(), 16)},
            }
        )

        return response

    def get(self, secret_id: str) -> str:
        known_versions = self.list_versions(secret_id)
        if not known_versions:
            raise NoSecretVersionAvailable()
        secret_id = known_versions[0].name
        return self.client.access_secret_version(request={"name": secret_id}).payload.data.decode("utf-8")

    def list_versions(self, secret_id: str) -> List[SecretVersion]:
        parent_id = self.client.secret_path(self.project_id, secret_id)
        try:
            return [version for version in self.client.list_secret_versions(parent=parent_id)]
        except NotFound as e:
            raise SecretNotFound() from e
