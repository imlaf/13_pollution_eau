import logging
import os

import boto3

logger = logging.getLogger(__name__)


def download_database():
    try:
        env = os.getenv("ENV")
        bucket = os.getenv("S3_BUCKET")
        endpoint = os.getenv("S3_ENDPOINT")
        s3_key = f"{env}/database.duckdb"
        local_path = "./../../database/database_downloaded.duckdb"

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

        s3.download_file(bucket, s3_key, local_path)
        logger.info(
            f"✅ Base téléchargée depuis s3://{bucket}/{s3_key} -> {local_path}"
        )

    except Exception as e:
        logger.error(f"Erreur lors du téléchargement: {e}")


def execute():
    download_database()
