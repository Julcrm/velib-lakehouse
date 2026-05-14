"""
Ressource Dagster partagée pour la connexion à MinIO.
Centralisée ici pour être réutilisée par tous les projets futurs.
"""
import json

import s3fs
from dagster import ConfigurableResource


class MinioResource(ConfigurableResource):
    endpoint: str
    access_key: str
    secret_key: str

    def get_filesystem(self) -> s3fs.S3FileSystem:
        """Retourne un filesystem s3fs configuré pour MinIO."""
        return s3fs.S3FileSystem(
            key=self.access_key,
            secret=self.secret_key,
            endpoint_url=self.endpoint,
            client_kwargs={"endpoint_url": self.endpoint},
        )

    def upload_json(self, bucket: str, key: str, data: dict) -> str:
        """Upload un dict JSON sur MinIO. Retourne le chemin s3://."""
        fs = self.get_filesystem()
        path = f"{bucket}/{key}"
        with fs.open(path, "w") as f:
            json.dump(data, f)
        return f"s3://{path}"
