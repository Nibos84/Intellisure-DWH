import logging
import boto3
from botocore.exceptions import ClientError
from typing import Optional, List, Union
import io
from .config import config, Environment

logger = logging.getLogger(__name__)

class S3Manager:
    """
    Manages interactions with OVH Object Storage (S3).
    Handles authentication and bucket selection via Config.
    """
    def __init__(self):
        self.bucket_name = config.bucket_name
        self.s3_client = boto3.client(
            's3',
            endpoint_url=config.ovh_endpoint,
            aws_access_key_id=config.ovh_access_key,
            aws_secret_access_key=config.ovh_secret_key,
            region_name=config.ovh_region
        )
        logger.info(f"Initialized S3Manager for bucket: {self.bucket_name} in env: {config.env}")

    # def _get_prefix(self) -> str:
    #     """Deprecated: Buckets are now separated by ENV."""
    #     return ""

    def upload_file(self, file_content: Union[str, bytes, io.BytesIO], key: str) -> bool:
        """
        Uploads content to S3.
        Key should be the full path within the bucket.
        """
        # full_key = f"{self._get_prefix()}{key}"
        full_key = key
        
        try:
            if isinstance(file_content, str):
                file_obj = io.BytesIO(file_content.encode('utf-8'))
            elif isinstance(file_content, bytes):
                file_obj = io.BytesIO(file_content)
            else:
                file_obj = file_content

            self.s3_client.upload_fileobj(file_obj, self.bucket_name, full_key)
            logger.info(f"Successfully uploaded to {self.bucket_name}/{full_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload to {self.bucket_name}/{full_key}: {e}")
            return False

    def list_files(self, prefix: str = "") -> List[str]:
        """Lists files under a prefix."""
        # full_prefix = f"{self._get_prefix()}{prefix}"
        full_prefix = prefix
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=full_prefix)
            if 'Contents' not in response:
                return []
            return [obj['Key'] for obj in response['Contents']]
        except ClientError as e:
            logger.error(f"Failed to list files with prefix {full_prefix}: {e}")
            return []

    def read_file(self, key: str) -> Optional[bytes]:
        """Reads file content from S3 as bytes."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
        except ClientError as e:
            logger.error(f"Failed to read file {self.bucket_name}/{key}: {e}")
            return None

    def check_connection(self) -> bool:
        """Simple check to verify connectivity."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError as e:
            logger.error(f"Connection check failed: {e}")
            return False

# Singleton
s3_manager = S3Manager()
