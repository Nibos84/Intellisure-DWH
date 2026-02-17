"""
Tests for S3 Credential Service.

Verifies that presigned URLs are generated correctly and do not expose credentials.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

from src.security.s3_credential_service import S3CredentialService


class TestS3CredentialService:
    """Test S3 credential service for presigned URL generation."""
    
    @pytest.fixture
    def s3_service(self):
        """Create S3 credential service instance."""
        return S3CredentialService(
            endpoint_url="https://s3.example.com",
            region_name="test-region",
            access_key="test-access-key",
            secret_key="test-secret-key",
            default_expiration=3600
        )
    
    @patch('src.security.s3_credential_service.boto3.client')
    def test_service_initialization(self, mock_boto_client):
        """Test that service initializes with correct parameters."""
        service = S3CredentialService(
            endpoint_url="https://s3.example.com",
            region_name="test-region",
            access_key="test-key",
            secret_key="test-secret",
            default_expiration=7200
        )
        
        assert service.endpoint_url == "https://s3.example.com"
        assert service.region_name == "test-region"
        assert service.default_expiration == 7200
        
        # Verify boto3 client was created with correct params
        mock_boto_client.assert_called_once_with(
            's3',
            endpoint_url="https://s3.example.com",
            region_name="test-region",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret"
        )
    
    @patch('src.security.s3_credential_service.boto3.client')
    def test_generate_presigned_upload_url(self, mock_boto_client, s3_service):
        """Test presigned upload URL generation."""
        mock_client = Mock()
        mock_client.generate_presigned_url.return_value = "https://s3.example.com/bucket/key?signature=xyz"
        mock_boto_client.return_value = mock_client
        
        # Recreate service with mocked client
        service = S3CredentialService(
            endpoint_url="https://s3.example.com",
            region_name="test-region",
            access_key="test-key",
            secret_key="test-secret"
        )
        
        url = service.generate_presigned_upload_url(
            bucket="test-bucket",
            key="test/path/file.json",
            expiration=1800
        )
        
        # Verify URL was generated
        assert url.startswith("https://s3.example.com")
        assert "signature=" in url
        
        # Verify correct boto3 call
        mock_client.generate_presigned_url.assert_called_once_with(
            'put_object',
            Params={
                'Bucket': 'test-bucket',
                'Key': 'test/path/file.json',
                'ContentType': 'application/json'
            },
            ExpiresIn=1800
        )
    
    @patch('src.security.s3_credential_service.boto3.client')
    def test_generate_presigned_download_url(self, mock_boto_client, s3_service):
        """Test presigned download URL generation."""
        mock_client = Mock()
        mock_client.generate_presigned_url.return_value = "https://s3.example.com/bucket/key?signature=abc"
        mock_boto_client.return_value = mock_client
        
        service = S3CredentialService(
            endpoint_url="https://s3.example.com",
            region_name="test-region",
            access_key="test-key",
            secret_key="test-secret"
        )
        
        url = service.generate_presigned_download_url(
            bucket="test-bucket",
            key="test/path/file.json",
            expiration=900
        )
        
        assert url.startswith("https://s3.example.com")
        assert "signature=" in url
        
        mock_client.generate_presigned_url.assert_called_once_with(
            'get_object',
            Params={
                'Bucket': 'test-bucket',
                'Key': 'test/path/file.json'
            },
            ExpiresIn=900
        )
    
    @patch('src.security.s3_credential_service.boto3.client')
    def test_presigned_url_no_credentials(self, mock_boto_client):
        """Verify presigned URLs don't contain raw credentials."""
        mock_client = Mock()
        mock_client.generate_presigned_url.return_value = (
            "https://s3.example.com/bucket/key?"
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&"
            "X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20230101%2Fus-east-1%2Fs3%2Faws4_request&"
            "X-Amz-Date=20230101T000000Z&"
            "X-Amz-Expires=3600&"
            "X-Amz-SignedHeaders=host&"
            "X-Amz-Signature=abcdef123456"
        )
        mock_boto_client.return_value = mock_client
        
        service = S3CredentialService(
            endpoint_url="https://s3.example.com",
            region_name="test-region",
            access_key="SECRET_ACCESS_KEY_12345",
            secret_key="SECRET_SECRET_KEY_67890"
        )
        
        url = service.generate_presigned_upload_url(
            bucket="test-bucket",
            key="test.json"
        )
        
        # Verify raw credentials are NOT in URL
        assert "SECRET_ACCESS_KEY_12345" not in url
        assert "SECRET_SECRET_KEY_67890" not in url
        
        # Verify signature IS in URL (credentials are hashed)
        assert "X-Amz-Signature=" in url
    
    @patch('src.security.s3_credential_service.boto3.client')
    def test_default_expiration_used(self, mock_boto_client):
        """Test that default expiration is used when not specified."""
        mock_client = Mock()
        mock_client.generate_presigned_url.return_value = "https://s3.example.com/url"
        mock_boto_client.return_value = mock_client
        
        service = S3CredentialService(
            endpoint_url="https://s3.example.com",
            region_name="test-region",
            access_key="key",
            secret_key="secret",
            default_expiration=7200  # 2 hours
        )
        
        service.generate_presigned_upload_url(
            bucket="bucket",
            key="key"
            # No expiration specified
        )
        
        # Verify default expiration was used
        call_args = mock_client.generate_presigned_url.call_args
        assert call_args[1]['ExpiresIn'] == 7200
    
    @patch('src.security.s3_credential_service.boto3.client')
    def test_custom_content_type(self, mock_boto_client):
        """Test custom content type for upload URL."""
        mock_client = Mock()
        mock_client.generate_presigned_url.return_value = "https://s3.example.com/url"
        mock_boto_client.return_value = mock_client
        
        service = S3CredentialService(
            endpoint_url="https://s3.example.com",
            region_name="test-region",
            access_key="key",
            secret_key="secret"
        )
        
        service.generate_presigned_upload_url(
            bucket="bucket",
            key="file.csv",
            content_type="text/csv"
        )
        
        call_args = mock_client.generate_presigned_url.call_args
        assert call_args[1]['Params']['ContentType'] == "text/csv"
    
    @patch('src.security.s3_credential_service.boto3.client')
    def test_client_error_handling(self, mock_boto_client):
        """Test that ClientError is properly raised."""
        mock_client = Mock()
        mock_client.generate_presigned_url.side_effect = ClientError(
            {'Error': {'Code': '500', 'Message': 'Internal Error'}},
            'generate_presigned_url'
        )
        mock_boto_client.return_value = mock_client
        
        service = S3CredentialService(
            endpoint_url="https://s3.example.com",
            region_name="test-region",
            access_key="key",
            secret_key="secret"
        )
        
        with pytest.raises(ClientError):
            service.generate_presigned_upload_url(
                bucket="bucket",
                key="key"
            )
    
    @patch('src.security.s3_credential_service.boto3.client')
    def test_verify_object_exists(self, mock_boto_client):
        """Test object existence verification."""
        mock_client = Mock()
        mock_client.head_object.return_value = {'ContentLength': 1024}
        mock_boto_client.return_value = mock_client
        
        service = S3CredentialService(
            endpoint_url="https://s3.example.com",
            region_name="test-region",
            access_key="key",
            secret_key="secret"
        )
        
        exists = service.verify_object_exists(
            bucket="bucket",
            key="existing-file.json"
        )
        
        assert exists is True
        mock_client.head_object.assert_called_once_with(
            Bucket="bucket",
            Key="existing-file.json"
        )
    
    @patch('src.security.s3_credential_service.boto3.client')
    def test_verify_object_not_exists(self, mock_boto_client):
        """Test object existence verification for non-existent object."""
        mock_client = Mock()
        mock_client.head_object.side_effect = ClientError(
            {'Error': {'Code': '404', 'Message': 'Not Found'}},
            'head_object'
        )
        mock_boto_client.return_value = mock_client
        
        service = S3CredentialService(
            endpoint_url="https://s3.example.com",
            region_name="test-region",
            access_key="key",
            secret_key="secret"
        )
        
        exists = service.verify_object_exists(
            bucket="bucket",
            key="non-existent.json"
        )
        
        assert exists is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
