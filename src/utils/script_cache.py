"""
Script caching utility for pipeline scripts.

Caches generated scripts based on manifest hash to avoid redundant LLM calls.
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class ScriptCache:
    """
    Cache for generated pipeline scripts.
    
    Caches scripts based on a hash of the pipeline manifest to avoid
    redundant LLM calls for identical configurations.
    
    Benefits:
    - Reduced LLM costs (no repeated script generation)
    - Lower latency (cached scripts instantly available)
    - Consistency (same manifest = same script)
    """
    
    def __init__(self, cache_dir: str = "cache/scripts", ttl_days: int = 30):
        """
        Initialize script cache.
        
        Args:
            cache_dir: Directory to store cached scripts
            ttl_days: Time-to-live for cached scripts in days (default: 30)
        """
        self.cache_dir = Path(cache_dir)
        self.ttl_days = ttl_days
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"ScriptCache initialized: dir={cache_dir}, ttl={ttl_days} days")
    
    def _generate_cache_key(self, manifest: Dict[str, Any]) -> str:
        """
        Generate cache key from manifest hash.
        
        Args:
            manifest: Pipeline manifest dictionary
            
        Returns:
            SHA256 hash of manifest (first 16 chars)
        """
        # Sort keys for consistent hashing
        manifest_str = json.dumps(manifest, sort_keys=True)
        hash_obj = hashlib.sha256(manifest_str.encode('utf-8'))
        cache_key = hash_obj.hexdigest()[:16]  # First 16 chars
        
        return cache_key
    
    def get(self, manifest: Dict[str, Any]) -> Optional[str]:
        """
        Retrieve cached script for manifest.
        
        Args:
            manifest: Pipeline manifest
            
        Returns:
            Cached script content if found and not expired, None otherwise
        """
        cache_key = self._generate_cache_key(manifest)
        cache_file = self.cache_dir / f"{cache_key}.py"
        metadata_file = self.cache_dir / f"{cache_key}.meta.json"
        
        if not cache_file.exists() or not metadata_file.exists():
            logger.debug(f"Cache MISS: {cache_key}")
            return None
        
        # Check TTL
        try:
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            
            cached_at = datetime.fromisoformat(metadata['cached_at'])
            expiry = cached_at + timedelta(days=self.ttl_days)
            
            if datetime.now() > expiry:
                logger.info(f"Cache EXPIRED: {cache_key} (cached at {cached_at})")
                # Clean up expired cache
                cache_file.unlink(missing_ok=True)
                metadata_file.unlink(missing_ok=True)
                return None
            
            # Cache hit!
            with open(cache_file, 'r') as f:
                script_content = f.read()
            
            logger.info(
                f"Cache HIT: {cache_key} "
                f"(cached {(datetime.now() - cached_at).days} days ago)"
            )
            
            return script_content
            
        except Exception as e:
            logger.warning(f"Cache read error for {cache_key}: {e}")
            return None
    
    def set(self, manifest: Dict[str, Any], script_content: str) -> None:
        """
        Store script in cache.
        
        Args:
            manifest: Pipeline manifest
            script_content: Generated script content
        """
        cache_key = self._generate_cache_key(manifest)
        cache_file = self.cache_dir / f"{cache_key}.py"
        metadata_file = self.cache_dir / f"{cache_key}.meta.json"
        
        try:
            # Write script
            with open(cache_file, 'w') as f:
                f.write(script_content)
            
            # Write metadata
            metadata = {
                'cached_at': datetime.now().isoformat(),
                'manifest_hash': cache_key,
                'pipeline_name': manifest.get('pipeline_name', 'unknown'),
                'agent_type': manifest.get('agent_type', 'unknown')
            }
            
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"Cache STORED: {cache_key} for pipeline '{metadata['pipeline_name']}'")
            
        except Exception as e:
            logger.error(f"Cache write error for {cache_key}: {e}")
    
    def clear(self) -> int:
        """
        Clear all cached scripts.
        
        Returns:
            Number of cache entries removed
        """
        count = 0
        for file in self.cache_dir.glob("*"):
            file.unlink()
            count += 1
        
        logger.info(f"Cache cleared: {count} files removed")
        return count // 2  # Each entry has 2 files (.py + .meta.json)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache stats
        """
        cache_files = list(self.cache_dir.glob("*.py"))
        
        total_size = sum(f.stat().st_size for f in cache_files)
        
        return {
            'total_entries': len(cache_files),
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'cache_dir': str(self.cache_dir),
            'ttl_days': self.ttl_days
        }


# Global cache instance
_cache_instance: Optional[ScriptCache] = None


def get_script_cache() -> ScriptCache:
    """Get or create global script cache instance."""
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = ScriptCache()
    return _cache_instance
