import logging
import os

from pipelines.utils.storage_client import ObjectStorageClient

logger = logging.getLogger(__name__)


def upload():
    s3 = ObjectStorageClient()

    try:
        local_db_path = "./../../database/data.duckdb"  # Fichier local
        remote_s3_key = f"{os.getenv('ENV')}/database.duckdb"  # Destination sur S3

        s3.upload_object(local_db_path, remote_s3_key)
        logger.info(f"✅ Base uploadée sur s3://{s3.bucket_name}/{remote_s3_key}")

    except Exception as e:
        logger.error(f"Erreur lors de l'upload: {e}")


def execute():
    upload()
