"""
Tests for manifest schema validation.

Verifies that URL validation properly blocks private/local URLs
and allows public URLs.
"""
import pytest
from pydantic import ValidationError
from src.schemas.manifest_schemas import validate_manifest, SourceConfig


def test_url_validation_blocks_localhost():
    """Test that localhost URLs are blocked."""
    invalid_urls = [
        "http://localhost:8080/api",
        "http://127.0.0.1/data",
        "http://0.0.0.0:5000",
        "http://[::1]/api",  # IPv6 localhost
    ]
    
    for url in invalid_urls:
        with pytest.raises(ValidationError) as exc_info:
            validate_manifest({
                "pipeline_name": "test",
                "agent_type": "generic_rest_api",
                "source": {"url": url, "method": "GET"},
                "target": {"bucket": "test", "layer": "landing", "source": "test", "dataset": "data"}
            })
        
        error_msg = str(exc_info.value)
        assert "Localhost URLs not allowed" in error_msg, f"Expected localhost error for {url}"


def test_url_validation_blocks_private_ips():
    """Test that private IP ranges are blocked."""
    invalid_urls = [
        "http://10.0.0.1/api",        # Private Class A
        "http://172.16.0.1/data",     # Private Class B
        "http://192.168.1.1/endpoint", # Private Class C
        "http://169.254.0.1/api",     # Link-local
    ]
    
    for url in invalid_urls:
        with pytest.raises(ValidationError) as exc_info:
            validate_manifest({
                "pipeline_name": "test",
                "agent_type": "generic_rest_api",
                "source": {"url": url, "method": "GET"},
                "target": {"bucket": "test", "layer": "landing", "source": "test", "dataset": "data"}
            })
        
        error_msg = str(exc_info.value)
        assert "Private/reserved IP address not allowed" in error_msg, f"Expected private IP error for {url}"


def test_url_validation_blocks_private_hostnames():
    """Test that private hostname patterns are blocked."""
    invalid_urls = [
        "http://internal.company.com/api",
        "http://corp-server.local/data",
        "http://intranet.example.com/endpoint",
        "http://server.lan/api",
    ]
    
    for url in invalid_urls:
        with pytest.raises(ValidationError) as exc_info:
            validate_manifest({
                "pipeline_name": "test",
                "agent_type": "generic_rest_api",
                "source": {"url": url, "method": "GET"},
                "target": {"bucket": "test", "layer": "landing", "source": "test", "dataset": "data"}
            })
        
        error_msg = str(exc_info.value)
        assert "Private hostname pattern detected" in error_msg, f"Expected private hostname error for {url}"


def test_url_validation_allows_public_urls():
    """Test that legitimate public URLs are allowed."""
    valid_urls = [
        "https://api.data.gov/api/v1/data",
        "https://opendata.cbs.nl/ODataApi/odata/table",
        "https://data.knmi.nl/datasets",
        "https://dummyjson.com/products",
        "https://example.com/api",
        "http://example.org/data",
    ]
    
    for url in valid_urls:
        # Should not raise
        validated = validate_manifest({
            "pipeline_name": "test",
            "agent_type": "generic_rest_api",
            "source": {"url": url, "method": "GET"},
            "target": {"bucket": "test", "layer": "landing", "source": "test", "dataset": "data"}
        })
        assert validated.source.url is not None, f"URL {url} should be valid"


def test_url_validation_none_url():
    """Test that None URL is allowed for non-ingestion sources."""
    # This should work since URL is optional in SourceConfig
    # but will fail validation at IngestionManifestSchema level
    source = SourceConfig(type="rest_api", url=None)
    assert source.url is None


def test_source_config_direct_validation():
    """Test SourceConfig URL validation directly."""
    # Valid public URL
    source = SourceConfig(
        type="rest_api",
        url="https://example.com/api"
    )
    assert source.url is not None
    
    # Invalid localhost URL
    with pytest.raises(ValidationError) as exc_info:
        SourceConfig(
            type="rest_api",
            url="http://localhost:8080/api"
        )
    assert "Localhost URLs not allowed" in str(exc_info.value)
    
    # Invalid private IP
    with pytest.raises(ValidationError) as exc_info:
        SourceConfig(
            type="rest_api",
            url="http://192.168.1.1/api"
        )
    assert "Private/reserved IP address not allowed" in str(exc_info.value)


def test_ingestion_manifest_requires_url():
    """Test that ingestion manifests require a URL."""
    with pytest.raises(ValidationError) as exc_info:
        validate_manifest({
            "pipeline_name": "test",
            "agent_type": "generic_rest_api",
            "source": {"method": "GET"},  # Missing URL
            "target": {"bucket": "test", "layer": "landing", "source": "test", "dataset": "data"}
        })
    
    error_msg = str(exc_info.value)
    # Could be either "URL is required" or a field required error
    assert "url" in error_msg.lower() or "required" in error_msg.lower()


def test_pipeline_name_validation():
    """Test pipeline name validation rules."""
    # Valid pipeline names
    valid_names = ["test_pipeline", "my_pipeline_123", "rechtspraak_daily"]
    
    for name in valid_names:
        validated = validate_manifest({
            "pipeline_name": name,
            "agent_type": "generic_rest_api",
            "source": {"url": "https://example.com/api", "method": "GET"},
            "target": {"bucket": "test", "layer": "landing", "source": "test", "dataset": "data"}
        })
        assert validated.pipeline_name == name
    
    # Invalid pipeline names
    invalid_names = [
        "My-Pipeline",      # Uppercase and hyphens
        "_pipeline",        # Starts with underscore
        "pipeline_",        # Ends with underscore
        "my__pipeline",     # Consecutive underscores
        "ab",              # Too short
    ]
    
    for name in invalid_names:
        with pytest.raises(ValidationError):
            validate_manifest({
                "pipeline_name": name,
                "agent_type": "generic_rest_api",
                "source": {"url": "https://example.com/api", "method": "GET"},
                "target": {"bucket": "test", "layer": "landing", "source": "test", "dataset": "data"}
            })
