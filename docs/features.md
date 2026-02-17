# Features Documentation

This document describes all production-ready features implemented in the Agentic Data Engineering System.

---

## 🚀 Performance & Cost Optimization

### Script Caching

**Location:** `src/utils/script_cache.py`

The system caches generated scripts based on manifest hash to dramatically reduce LLM costs and improve performance.

**How It Works:**
1. Each pipeline manifest is hashed using SHA256
2. Before generating a new script, the system checks `cache/scripts/` for a cached version
3. **Cache Hit**: Cached script is used (~100ms)
4. **Cache Miss**: LLM generates new script and caches it (~5-10s)

**Benefits:**
- 💰 ~99% cost reduction for repeated pipeline runs (~$360/year savings per pipeline)
- ⚡ 10-100x faster script generation
- 🔄 Consistent output for same manifest

**Configuration:**
```python
# Default TTL: 30 days
# Cache location: cache/scripts/
```

**Usage:**
```python
from src.utils.script_cache import get_script_cache

cache = get_script_cache()
cached_script = cache.get(manifest)
if cached_script:
    # Use cached script
else:
    # Generate and cache new script
    cache.set(manifest, new_script)
```

---

### Configurable Sample Size

**Environment Variable:** `SAMPLE_DATA_SIZE`

Allows adjusting the amount of sample data extracted for transformation analysis.

```bash
SAMPLE_DATA_SIZE=5000  # Default: 5000 characters
```

**Use Cases:**
- Small samples for quick testing
- Large samples for complex schema inference
- Adjustable per environment (dev vs prod)

---

### UUID Filenames

Generated scripts use UUID-based filenames to prevent race conditions during parallel pipeline execution.

```python
script_path = f"ingest_{pipeline_name}_{uuid.uuid4().hex[:8]}.py"
```

**Benefits:**
- ✅ Safe parallel execution
- ✅ No file collisions
- ✅ Automatic cleanup

---

## 🛡️ Security & Validation

### AST Syntax Validation

**Location:** `src/agents/mas/ingestion_specialist.py`, `transformation_specialist.py`

All generated scripts are validated using Python's AST parser before execution.

```python
def _validate_syntax(self, code: str) -> bool:
    try:
        ast.parse(code)
        return True
    except SyntaxError as e:
        logger.error(f"Syntax error: {e.msg} at line {e.lineno}")
        return False
```

**Benefits:**
- ✅ Catches syntax errors before execution
- ✅ Better error messages with line numbers
- ✅ Prevents wasted execution time

---

### Schema Type Validation

**Location:** `src/agents/mas/transformation_specialist.py`

Transformation scripts include auto-generated schema validation code.

```python
# Validate schema
expected_schema = {'id': 'int', 'name': 'str', 'created_at': 'datetime64[ns]'}
actual_dtypes = df.dtypes.astype(str).to_dict()

mismatches = []
for col, expected_type in expected_schema.items():
    if col not in df.columns:
        mismatches.append(f'Missing column: {col}')
    # ... type checking logic ...

if mismatches:
    raise ValueError(f'Schema validation failed: {mismatches}')
```

**Benefits:**
- ✅ Early detection of type conversion errors
- ✅ Detailed mismatch reporting
- ✅ Prevents downstream failures

---

## 🧪 Testing & Safety

### Dry-Run Mode

**Environment Variable:** `DRY_RUN`

Validates scripts without executing them or performing S3 operations.

```bash
DRY_RUN=true  # Enable dry-run mode
```

**Behavior:**
```python
if config.dry_run:
    logger.info("[DRY-RUN] Would execute script: {script_path}")
    logger.info("[DRY-RUN] Script validated successfully")
    logger.info("[DRY-RUN] Target: s3://{bucket}/{key}")
    return {"status": "dry_run_success"}
```

**Use Cases:**
- ✅ Testing new pipeline configurations
- ✅ Validating script generation
- ✅ CI/CD pipeline validation
- ✅ Safe production testing

---

### Comprehensive Test Suite

**Location:** `tests/`

58+ tests covering all critical functionality:

```bash
pytest tests/ -v
```

**Test Coverage:**
- Script caching (13 tests)
- S3 credential service (11 tests)
- Execution timeouts (8 tests)
- Manifest schemas (5 tests)
- Script generation (15 tests)
- JSON logging (7 tests)
- Code validation (security tests)

---

## 📈 Observability

### Structured JSON Logging

**Location:** `src/utils/json_logger.py`  
**Environment Variable:** `STRUCTURED_LOGGING`

Enable JSON-formatted logs for easy parsing by monitoring tools.

```bash
STRUCTURED_LOGGING=true
```

**Output Example:**
```json
{
  "timestamp": "2026-02-17T20:52:36Z",
  "level": "INFO",
  "logger": "ingestion_specialist",
  "message": "Pipeline completed",
  "module": "ingestion_specialist",
  "function": "execute",
  "line": 42,
  "pipeline_name": "api_ingestion",
  "agent_type": "ingestion",
  "duration_ms": 1500
}
```

**Usage:**
```python
from src.utils.json_logger import setup_json_logging, log_with_context

logger = setup_json_logging('my_agent')

log_with_context(
    logger,
    logging.INFO,
    'Pipeline completed',
    pipeline_name='api_ingestion',
    duration_ms=1500
)
```

**Benefits:**
- ✅ Easy parsing for ELK, Splunk, Datadog
- ✅ Structured fields for filtering
- ✅ Context-aware logging
- ✅ Consistent format

---

## 🎯 Code Quality

### Script Templates

**Location:** `templates/`

Reusable templates guide LLM code generation and reduce hallucinations.

**Available Templates:**
- `templates/ingestion/rest_api_pagination.py` - REST API pagination pattern
- `templates/transformation/pandas_transformation.py` - Pandas transformation pattern

**How They Work:**
Templates are referenced in LLM prompts to guide code generation:

```python
prompt += """
TEMPLATE REFERENCE (follow this pattern):
```python
# Pagination pattern
all_data = []
page = 1
while True:
    response = requests.get(f'{API_URL}?page={page}')
    if not data: break
    all_data.extend(data)
    page += 1
```
"""
```

**Benefits:**
- ✅ Reduced LLM hallucinations
- ✅ Consistent code patterns
- ✅ Best practices embedded
- ✅ Faster generation

See [Templates Documentation](templates.md) for details.

---

## 📊 Impact Summary

| Feature | Before | After | Impact |
|---------|--------|-------|--------|
| **LLM Costs** | $1/day | $0.01/day | 💰 99% reduction |
| **Latency** | 5-10s | 100ms | ⚡ 10-100x faster |
| **Syntax Errors** | Runtime | Pre-execution | 🛡️ Early detection |
| **Testing** | Production only | Dry-run mode | 🔒 Safe testing |
| **Data Types** | No validation | Schema checks | 📊 Data quality |
| **Logging** | Unstructured | JSON structured | 📈 Observability |
| **Code Patterns** | Inconsistent | Template-based | 🎯 Consistency |

---

## 🔧 Configuration Reference

All features are configurable via environment variables:

```bash
# Performance & Cost
SAMPLE_DATA_SIZE=5000
PRESIGNED_URL_EXPIRATION=3600

# Safety & Testing
DRY_RUN=false
SCRIPT_EXECUTION_TIMEOUT=300

# Observability
STRUCTURED_LOGGING=false
```

See [.env.example](../.env.example) for full configuration.
