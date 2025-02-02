import logging
import os
from pathlib import Path

from pipelines.utils.storage_client import ObjectStorageClient
from pipelines.utils.utils import get_project_root

logger = logging.getLogger(__name__)


def upload():
    s3 = ObjectStorageClient()

    try:
        local_db_path = Path(get_project_root(), "database/data.duckdb")
        remote_s3_key = f"{os.getenv('ENV')}/data.duckdb"  # Destination sur S3

        s3.upload_object(local_db_path, remote_s3_key)
        logger.info(f"✅ Base uploadée sur s3://{s3.bucket_name}/{remote_s3_key}")

    except Exception as e:
        logger.error(f"Erreur lors de l'upload: {e}")


def execute():
    upload()
