import logging
import os
from pathlib import Path

from pipelines.utils.storage_client import ObjectStorageClient
from pipelines.utils.utils import get_project_root

logger = logging.getLogger(__name__)


def download_database():
    s3 = ObjectStorageClient()
    try:
        env = os.getenv("ENV")
        remote_s3_key = f"{env}/data.duckdb"
        local_db_path = Path(get_project_root(), "database/database_downloaded.duckdb")

        s3.download_object(remote_s3_key, local_db_path)
        logger.info(
            f"✅ Base téléchargée depuis s3://{s3.bucket_name}/{remote_s3_key} -> {local_db_path}"
        )

    except Exception as e:
        logger.error(f"Erreur lors du téléchargement: {e}")


def execute():
    download_database()
