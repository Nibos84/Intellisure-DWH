"""
Tests for script caching functionality.
"""

import pytest
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from src.utils.script_cache import ScriptCache, get_script_cache


class TestScriptCache:
    """Test suite for ScriptCache class."""
    
    @pytest.fixture
    def temp_cache_dir(self, tmp_path):
        """Create temporary cache directory."""
        cache_dir = tmp_path / "test_cache"
        return str(cache_dir)
    
    @pytest.fixture
    def sample_manifest(self):
        """Sample pipeline manifest."""
        return {
            "pipeline_name": "test_pipeline",
            "agent_type": "ingestion",
            "source": {
                "type": "api",
                "url": "https://api.example.com/data"
            },
            "target": {
                "bucket": "test-bucket",
                "path": "landing/test"
            }
        }
    
    @pytest.fixture
    def sample_script(self):
        """Sample generated script."""
        return """
import requests
import os

url = os.environ['API_URL']
response = requests.get(url)
print(response.json())
"""
    
    def test_cache_initialization(self, temp_cache_dir):
        """Test cache initialization creates directory."""
        cache = ScriptCache(cache_dir=temp_cache_dir, ttl_days=30)
        
        assert Path(temp_cache_dir).exists()
        assert cache.ttl_days == 30
    
    def test_cache_miss(self, temp_cache_dir, sample_manifest):
        """Test cache miss returns None."""
        cache = ScriptCache(cache_dir=temp_cache_dir)
        
        result = cache.get(sample_manifest)
        
        assert result is None
    
    def test_cache_hit(self, temp_cache_dir, sample_manifest, sample_script):
        """Test cache hit returns cached script."""
        cache = ScriptCache(cache_dir=temp_cache_dir)
        
        # Store script
        cache.set(sample_manifest, sample_script)
        
        # Retrieve script
        result = cache.get(sample_manifest)
        
        assert result == sample_script
    
    def test_cache_key_generation(self, temp_cache_dir, sample_manifest):
        """Test cache key is consistent for same manifest."""
        cache = ScriptCache(cache_dir=temp_cache_dir)
        
        key1 = cache._generate_cache_key(sample_manifest)
        key2 = cache._generate_cache_key(sample_manifest)
        
        assert key1 == key2
        assert len(key1) == 16  # First 16 chars of SHA256
    
    def test_cache_key_different_manifests(self, temp_cache_dir, sample_manifest):
        """Test different manifests generate different keys."""
        cache = ScriptCache(cache_dir=temp_cache_dir)
        
        manifest2 = sample_manifest.copy()
        manifest2['pipeline_name'] = 'different_pipeline'
        
        key1 = cache._generate_cache_key(sample_manifest)
        key2 = cache._generate_cache_key(manifest2)
        
        assert key1 != key2
    
    def test_cache_expiration(self, temp_cache_dir, sample_manifest, sample_script):
        """Test cache expiration removes old entries."""
        cache = ScriptCache(cache_dir=temp_cache_dir, ttl_days=0)  # Expire immediately
        
        # Store script
        cache.set(sample_manifest, sample_script)
        
        # Manually modify metadata to simulate old cache
        cache_key = cache._generate_cache_key(sample_manifest)
        metadata_file = Path(temp_cache_dir) / f"{cache_key}.meta.json"
        
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        # Set cached_at to 2 days ago
        old_time = datetime.now() - timedelta(days=2)
        metadata['cached_at'] = old_time.isoformat()
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f)
        
        # Try to retrieve - should return None (expired)
        result = cache.get(sample_manifest)
        
        assert result is None
    
    def test_cache_metadata(self, temp_cache_dir, sample_manifest, sample_script):
        """Test cache stores metadata correctly."""
        cache = ScriptCache(cache_dir=temp_cache_dir)
        
        cache.set(sample_manifest, sample_script)
        
        cache_key = cache._generate_cache_key(sample_manifest)
        metadata_file = Path(temp_cache_dir) / f"{cache_key}.meta.json"
        
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        assert metadata['pipeline_name'] == 'test_pipeline'
        assert metadata['agent_type'] == 'ingestion'
        assert metadata['manifest_hash'] == cache_key
        assert 'cached_at' in metadata
    
    def test_cache_clear(self, temp_cache_dir, sample_manifest, sample_script):
        """Test cache clear removes all entries."""
        cache = ScriptCache(cache_dir=temp_cache_dir)
        
        # Store multiple scripts
        cache.set(sample_manifest, sample_script)
        
        manifest2 = sample_manifest.copy()
        manifest2['pipeline_name'] = 'pipeline2'
        cache.set(manifest2, sample_script)
        
        # Clear cache
        count = cache.clear()
        
        assert count == 2  # 2 entries removed
        assert cache.get(sample_manifest) is None
        assert cache.get(manifest2) is None
    
    def test_cache_stats(self, temp_cache_dir, sample_manifest, sample_script):
        """Test cache statistics."""
        cache = ScriptCache(cache_dir=temp_cache_dir, ttl_days=30)
        
        # Store script
        cache.set(sample_manifest, sample_script)
        
        stats = cache.get_stats()
        
        assert stats['total_entries'] == 1
        assert stats['total_size_bytes'] > 0
        assert stats['cache_dir'] == temp_cache_dir
        assert stats['ttl_days'] == 30
    
    def test_global_cache_instance(self):
        """Test global cache instance is singleton."""
        cache1 = get_script_cache()
        cache2 = get_script_cache()
        
        assert cache1 is cache2
    
    def test_cache_with_complex_manifest(self, temp_cache_dir, sample_script):
        """Test cache with complex nested manifest."""
        cache = ScriptCache(cache_dir=temp_cache_dir)
        
        complex_manifest = {
            "pipeline_name": "complex_pipeline",
            "agent_type": "transformation",
            "source": {
                "bucket": "source-bucket",
                "path": "landing/data",
                "format": "json"
            },
            "target": {
                "bucket": "target-bucket",
                "path": "staging/transformed",
                "format": "parquet"
            },
            "ai_config": {
                "instruction": "Transform data",
                "schema": {
                    "id": "int",
                    "name": "str",
                    "created_at": "datetime64[ns]"
                }
            }
        }
        
        # Store and retrieve
        cache.set(complex_manifest, sample_script)
        result = cache.get(complex_manifest)
        
        assert result == sample_script
