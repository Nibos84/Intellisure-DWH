# Cache Directory

This directory stores cached scripts to reduce LLM costs and improve performance.

## Structure

```
cache/
└── scripts/
    ├── <hash>.py          # Cached script
    └── <hash>.meta.json   # Metadata (timestamp, manifest hash)
```

## How It Works

1. **Manifest Hashing**: Each pipeline manifest is hashed using SHA256
2. **Cache Lookup**: Before generating a new script, the system checks if a cached version exists
3. **Cache Hit**: If found and not expired (TTL: 30 days), the cached script is used (~100ms)
4. **Cache Miss**: If not found, LLM generates a new script and caches it (~5-10s)

## Benefits

- **Cost Reduction**: ~99% reduction in LLM costs for repeated pipeline runs
- **Performance**: 10-100x faster script generation
- **Consistency**: Same manifest always produces the same script

## Configuration

Cache TTL and location are configured in `src/utils/script_cache.py`.

## Cleanup

Expired cache entries are automatically removed when accessed. To manually clear the cache:

```bash
Remove-Item cache\scripts\* -Force
```
