import logging
import os

import boto3

logger = logging.getLogger(__name__)


def upload():
    try:
        db_path = "./../../database/data.duckdb"  # Fichier local
        s3_key = f"{os.getenv('ENV')}/database.duckdb"  # Destination sur S3

        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("ENDPOINT_URL"),
            aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
        )

        s3.upload_file(db_path, os.getenv("BUCKET"), s3_key)
        logger.info(f"✅ Base uploadée sur s3://{os.getenv('BUCKET')}/{s3_key}")

    except Exception as e:
        logger.error(f"Erreur lors de l'upload: {e}")


def execute():
    upload()
