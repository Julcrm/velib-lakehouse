"""
Shared Dagster resource for MinIO connectivity.
Centralised here so every future pipeline module can reuse it.
"""
import json

import s3fs

from dagster import ConfigurableResource


class MinioResource(ConfigurableResource):
    """Dagster resource that wraps an s3fs filesystem pointed at a MinIO bucket."""

    endpoint: str
    access_key: str
    secret_key: str

    def get_filesystem(self) -> s3fs.S3FileSystem:
        """Return an s3fs filesystem configured for this MinIO instance."""
        return s3fs.S3FileSystem(
            key=self.access_key,
            secret=self.secret_key,
            endpoint_url=self.endpoint,
            client_kwargs={"endpoint_url": self.endpoint},
        )

    def upload_json(self, bucket: str, key: str, data: dict) -> str:
        """Upload a dict as JSON to MinIO and return the s3:// path."""
        fs = self.get_filesystem()
        path = f"{bucket}/{key}"
        with fs.open(path, "w") as f:
            json.dump(data, f)
        return f"s3://{path}"
