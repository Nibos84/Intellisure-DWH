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
        assert "Private/reserved" in error_msg and "not allowed" in error_msg, \
            f"Expected private IP error for {url}"


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
    error_msg = str(exc_info.value)
    assert "Private/reserved" in error_msg and "not allowed" in error_msg


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


def test_url_validation_blocks_ipv6_private_ranges():
    """Test that private IPv6 ranges are blocked."""
    invalid_urls = [
        # Link-local addresses (fe80::/10)
        "http://[fe80::1]/api",
        "http://[fe80::abcd:1234]/data",
        "http://[fe80::1]:8080/endpoint",
        
        # Unique Local Addresses (fc00::/7)
        "http://[fc00::1]/api",
        "http://[fd00::1234:5678]/data",
        "http://[fd12:3456:789a::1]/endpoint",
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
        assert ("Private/reserved" in error_msg or "Private IPv6" in error_msg), \
            f"Expected private IPv6 error for {url}, got: {error_msg}"


def test_url_validation_blocks_ipv6_localhost_variants():
    """Test that IPv6 localhost variants are blocked."""
    invalid_urls = [
        "http://[::1]/api",
        "http://[::1]:8080/data",
        "http://[0000:0000:0000:0000:0000:0000:0000:0001]/api",  # Expanded form
        "http://[::ffff:127.0.0.1]/api",  # IPv4-mapped IPv6 localhost
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
        assert "not allowed" in error_msg.lower(), f"Expected localhost error for {url}"


def test_url_validation_blocks_ipv6_multicast():
    """Test that IPv6 multicast addresses are blocked."""
    invalid_urls = [
        "http://[ff00::1]/api",  # Multicast
        "http://[ff02::1]/api",  # Link-local multicast
        "http://[ff05::1]/api",  # Site-local multicast
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
        assert "Private/reserved" in error_msg, f"Expected reserved IPv6 error for {url}"


def test_url_validation_blocks_ipv4_mapped_ipv6_private():
    """Test that IPv4-mapped IPv6 private addresses are blocked."""
    invalid_urls = [
        "http://[::ffff:10.0.0.1]/api",        # Private Class A
        "http://[::ffff:172.16.0.1]/data",     # Private Class B
        "http://[::ffff:192.168.1.1]/endpoint", # Private Class C
        "http://[::ffff:169.254.0.1]/api",     # Link-local
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
        assert "not allowed" in error_msg.lower(), \
            f"Expected private IP error for IPv4-mapped IPv6: {url}"


def test_url_validation_allows_public_ipv6():
    """Test that legitimate public IPv6 addresses are allowed."""
    valid_urls = [
        "http://[2001:4860:4860::8888]/api",  # Google Public DNS
        "http://[2606:4700:4700::1111]/data", # Cloudflare DNS
        "http://[2001:db8::1]/endpoint",      # Documentation prefix (but not reserved for private use)
    ]
    
    for url in valid_urls:
        # Note: 2001:db8::/32 is reserved for documentation, but ipaddress module
        # doesn't flag it as private/reserved in the same way as RFC 1918 addresses.
        # We'll test it anyway to ensure our validation isn't overly restrictive.
        try:
            validated = validate_manifest({
                "pipeline_name": "test",
                "agent_type": "generic_rest_api",
                "source": {"url": url, "method": "GET"},
                "target": {"bucket": "test", "layer": "landing", "source": "test", "dataset": "data"}
            })
            # If validation succeeds, ensure URL is preserved
            assert validated.source.url is not None
        except ValidationError as e:
            # If 2001:db8:: is blocked as reserved, that's acceptable security posture
            if "2001:db8" not in url:
                raise  # Other addresses must not be blocked
