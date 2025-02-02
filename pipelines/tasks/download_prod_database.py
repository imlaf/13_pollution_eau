import logging
import os

from pipelines.utils.storage_client import ObjectStorageClient

logger = logging.getLogger(__name__)


def download_database():
    s3 = ObjectStorageClient()
    try:
        env = os.getenv("ENV")
        remote_s3_key = f"{env}/database.duckdb"
        local_db_path = "./../../database/database_downloaded.duckdb"

        s3.download_object(remote_s3_key, local_db_path)
        logger.info(
            f"✅ Base téléchargée depuis s3://{s3.bucket_name}/{remote_s3_key} -> {local_db_path}"
        )

    except Exception as e:
        logger.error(f"Erreur lors du téléchargement: {e}")


def execute():
    download_database()
