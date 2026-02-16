"""
Additional tests for IPv6 URL validation.
"""

import pytest
from pydantic import ValidationError
from src.schemas.manifest_schemas import validate_manifest, SourceConfig


class TestIPv6URLValidation:
    """Test IPv6 URL validation."""
    
    def test_ipv6_localhost_blocked(self):
        """Should block IPv6 localhost addresses."""
        ipv6_localhost_urls = [
            "http://[::1]/api",                    # IPv6 localhost
            "http://[::ffff:127.0.0.1]/api",      # IPv4-mapped IPv6 localhost
        ]
        
        for url in ipv6_localhost_urls:
            with pytest.raises(ValidationError) as exc_info:
                SourceConfig(type="rest_api", url=url)
            
            error_msg = str(exc_info.value)
            assert "Localhost URLs not allowed" in error_msg, f"Expected localhost error for {url}"
    
    def test_ipv6_link_local_blocked(self):
        """Should block IPv6 link-local addresses (fe80::/10)."""
        link_local_urls = [
            "http://[fe80::1]/api",
            "http://[fe80::abcd:ef01:2345:6789]/data",
        ]
        
        for url in link_local_urls:
            with pytest.raises(ValidationError) as exc_info:
                SourceConfig(type="rest_api", url=url)
            
            error_msg = str(exc_info.value)
            assert "Private/reserved IPv6 address not allowed" in error_msg, f"Expected link-local error for {url}"
    
    def test_ipv6_unique_local_blocked(self):
        """Should block IPv6 Unique Local Addresses (fc00::/7)."""
        ula_urls = [
            "http://[fc00::1]/api",                # ULA
            "http://[fd00::1]/data",               # ULA
            "http://[fc12:3456::/64]/endpoint",    # ULA
        ]
        
        for url in ula_urls:
            with pytest.raises(ValidationError) as exc_info:
                SourceConfig(type="rest_api", url=url)
            
            error_msg = str(exc_info.value)
            assert "Private IPv6 address (ULA) not allowed" in error_msg, f"Expected ULA error for {url}"
    
    def test_ipv6_public_allowed(self):
        """Should allow public IPv6 addresses."""
        public_ipv6_urls = [
            "http://[2001:4860:4860::8888]/api",   # Google Public DNS
            "http://[2606:4700:4700::1111]/data",  # Cloudflare DNS
        ]
        
        for url in public_ipv6_urls:
            # Should not raise
            source = SourceConfig(type="rest_api", url=url)
            assert source.url is not None, f"Public IPv6 URL should be valid: {url}"


class TestIPv4MappedIPv6:
    """Test IPv4-mapped IPv6 addresses."""
    
    def test_ipv4_mapped_private_ips_blocked(self):
        """Should block IPv4-mapped IPv6 private addresses."""
        mapped_private_urls = [
            "http://[::ffff:192.168.1.1]/api",     # IPv4-mapped private
            "http://[::ffff:10.0.0.1]/data",       # IPv4-mapped private
        ]
        
        for url in mapped_private_urls:
            with pytest.raises(ValidationError) as exc_info:
                SourceConfig(type="rest_api", url=url)
            
            error_msg = str(exc_info.value)
            assert "not allowed" in error_msg, f"Expected private IP error for {url}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
