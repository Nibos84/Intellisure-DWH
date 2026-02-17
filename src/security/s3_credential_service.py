"""
S3 Credential Service for secure credential management.

Generates pre-signed URLs for S3 operations to prevent credential exposure
in LLM-generated scripts.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3CredentialService:
    """
    Generate pre-signed URLs for S3 operations without exposing credentials.
    
    This service creates time-limited URLs that allow specific S3 operations
    (upload, download) without requiring scripts to have access to raw credentials.
    
    Security Benefits:
    - Scripts never see OVH_ACCESS_KEY or OVH_SECRET_KEY
    - URLs are time-limited (default: 1 hour)
    - URLs are operation-specific (upload ≠ download)
    - All URL generation is logged for audit trail
    """
    
    def __init__(
        self,
        endpoint_url: str,
        region_name: str,
        access_key: str,
        secret_key: str,
        default_expiration: int = 3600
    ):
        """
        Initialize S3 credential service.
        
        Args:
            endpoint_url: S3 endpoint URL (e.g., https://s3.rbx.io.cloud.ovh.net/)
            region_name: S3 region (e.g., 'rbx')
            access_key: OVH access key (kept secure in service)
            secret_key: OVH secret key (kept secure in service)
            default_expiration: Default URL expiration in seconds (default: 3600 = 1 hour)
        """
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.default_expiration = default_expiration
        
        # Initialize S3 client with credentials (kept in service, not exposed)
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            region_name=region_name,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        
        logger.info(f"S3CredentialService initialized with endpoint: {endpoint_url}")
    
    def generate_presigned_upload_url(
        self,
        bucket: str,
        key: str,
        expiration: Optional[int] = None,
        content_type: str = 'application/json'
    ) -> str:
        """
        Generate a pre-signed URL for uploading an object to S3.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key (path)
            expiration: URL expiration in seconds (default: service default)
            content_type: Content type for upload (default: application/json)
            
        Returns:
            Pre-signed URL for PUT operation
            
        Raises:
            ClientError: If URL generation fails
            
        Example:
            >>> service = S3CredentialService(...)
            >>> url = service.generate_presigned_upload_url(
            ...     bucket='my-bucket',
            ...     key='landing/api/data.json',
            ...     expiration=3600
            ... )
            >>> # Script can now: requests.put(url, data=json_data)
        """
        expiration = expiration or self.default_expiration
        
        try:
            url = self.s3_client.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': bucket,
                    'Key': key,
                    'ContentType': content_type
                },
                ExpiresIn=expiration
            )
            
            # Audit log
            expires_at = datetime.now() + timedelta(seconds=expiration)
            logger.info(
                f"Generated presigned UPLOAD URL: "
                f"bucket={bucket}, key={key}, expires_at={expires_at.isoformat()}"
            )
            
            return url
            
        except ClientError as e:
            logger.error(f"Failed to generate presigned upload URL: {e}")
            raise
    
    def generate_presigned_download_url(
        self,
        bucket: str,
        key: str,
        expiration: Optional[int] = None
    ) -> str:
        """
        Generate a pre-signed URL for downloading an object from S3.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key (path)
            expiration: URL expiration in seconds (default: service default)
            
        Returns:
            Pre-signed URL for GET operation
            
        Raises:
            ClientError: If URL generation fails
            
        Example:
            >>> service = S3CredentialService(...)
            >>> url = service.generate_presigned_download_url(
            ...     bucket='my-bucket',
            ...     key='landing/api/data.json'
            ... )
            >>> # Script can now: requests.get(url)
        """
        expiration = expiration or self.default_expiration
        
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': bucket,
                    'Key': key
                },
                ExpiresIn=expiration
            )
            
            # Audit log
            expires_at = datetime.now() + timedelta(seconds=expiration)
            logger.info(
                f"Generated presigned DOWNLOAD URL: "
                f"bucket={bucket}, key={key}, expires_at={expires_at.isoformat()}"
            )
            
            return url
            
        except ClientError as e:
            logger.error(f"Failed to generate presigned download URL: {e}")
            raise
    
    def verify_object_exists(self, bucket: str, key: str) -> bool:
        """
        Verify that an S3 object exists before generating download URL.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            
        Returns:
            True if object exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
