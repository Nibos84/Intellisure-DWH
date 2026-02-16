# Input Validation Documentation

## Overview

The Agentic Data Engineering System uses **Pydantic v2** schemas to validate manifest YAML files before execution. This ensures data integrity, prevents runtime errors, and enforces business rules.

---

## Why Input Validation?

### Problems Without Validation
- Runtime errors from missing fields
- Type mismatches (string vs int)
- Invalid configurations (negative values, malformed URLs)
- Difficult-to-debug failures
- Security vulnerabilities from malformed input

### Benefits With Validation
- ✅ Catch errors before execution
- ✅ Clear, actionable error messages
- ✅ Type safety across the system
- ✅ Business rule enforcement
- ✅ Self-documenting schemas

---

## Schema Architecture

### Schema Hierarchy

```
IngestionManifestSchema
├── pipeline_name: str
├── agent_type: "generic_rest_api"
├── source: SourceConfig
│   ├── url: HttpUrl (required)
│   ├── method: "GET" | "POST" | "PUT"
│   ├── format: "json" | "xml" | "csv"
│   └── pagination: PaginationConfig (optional)
└── target: TargetConfig
    ├── bucket: str (3-63 chars, S3-compliant)
    ├── layer: "landing" | "silver" | "gold"
    ├── source: str (lowercase, numbers, underscores)
    └── dataset: str (lowercase, numbers, underscores)

TransformationManifestSchema
├── pipeline_name: str
├── agent_type: "generic_ai_transformer"
├── source: SourceConfig
│   ├── bucket: str (required)
│   └── path: str (required)
├── target: TargetConfig
│   └── path: str (required)
└── ai_config: AIConfig
    ├── instruction: str (10-5000 chars)
    └── schema: Dict[str, str] (min 1 field)
```

---

## Validation Rules

### Pipeline Names
**Pattern:** `^[a-z0-9_]+$`

**Rules:**
- Lowercase letters, numbers, underscores only
- Cannot start or end with underscore
- No consecutive underscores
- 3-100 characters

**Valid:**
```yaml
pipeline_name: "my_pipeline_123"
pipeline_name: "rechtspraak_daily"
```

**Invalid:**
```yaml
pipeline_name: "My-Pipeline"      # Uppercase, hyphens
pipeline_name: "_pipeline"        # Starts with underscore
pipeline_name: "my__pipeline"     # Consecutive underscores
```

### Bucket Names
**S3 Naming Rules:**
- 3-63 characters
- Lowercase letters, numbers, hyphens only
- Must start and end with letter or number
- No consecutive periods or period-hyphen combinations

**Valid:**
```yaml
bucket: "my-bucket"
bucket: "splendid-bethe"
bucket: "data-lake-2024"
```

**Invalid:**
```yaml
bucket: "ab"                  # Too short
bucket: "My-Bucket"           # Uppercase
bucket: "bucket-"             # Ends with hyphen
bucket: "my..bucket"          # Consecutive periods
```

### URLs
**Validation:** Pydantic `HttpUrl` type

**Valid:**
```yaml
url: "https://api.example.com/data"
url: "http://localhost:8000/api"
```

**Invalid:**
```yaml
url: "not-a-url"
url: "ftp://example.com"      # Only HTTP(S) allowed
```

### Pagination
**Types:** `offset`, `offset_limit`, `cursor`, `page`, `none`

**Rules:**
- `offset` and `offset_limit` require `offset_param`
- `limit_value` must be > 0 and ≤ 1000
- `max_items` must be > 0

**Valid:**
```yaml
pagination:
  type: "offset_limit"
  offset_param: "from"
  limit_param: "max"
  limit_value: 100
  max_items: 1000
```

**Invalid:**
```yaml
pagination:
  type: "offset"
  # Missing offset_param!
  
pagination:
  type: "offset"
  offset_param: "from"
  limit_value: -10          # Negative!
  
pagination:
  type: "offset"
  offset_param: "from"
  limit_value: 2000         # > 1000!
```

### AI Configuration
**Rules:**
- `instruction`: 10-5000 characters
- `schema`: At least 1 field
- Whitespace trimmed from instruction

**Valid:**
```yaml
ai_config:
  instruction: "Filter records where age > 18 and status = 'active'"
  schema:
    name: str
    age: int
    status: str
```

**Invalid:**
```yaml
ai_config:
  instruction: "Short"      # < 10 chars
  schema: {}                # Empty schema
```

---

## Usage

### Validating Manifests

```python
from src.schemas.manifest_schemas import validate_manifest
import yaml

# Load manifest
with open('manifest.yaml') as f:
    raw_config = yaml.safe_load(f)

# Validate
try:
    validated_schema = validate_manifest(raw_config)
    print(f"✅ Validation passed: {validated_schema.pipeline_name}")
except ValidationError as e:
    print(f"❌ Validation failed:")
    for error in e.errors():
        field = " -> ".join(str(loc) for loc in error['loc'])
        print(f"  • {field}: {error['msg']}")
```

### Automatic Validation in Runner

The `PipelineRunner` automatically validates manifests:

```python
from src.core.runner import PipelineRunner

# Validation happens automatically in __init__
runner = PipelineRunner('manifests/my_pipeline.yaml')

# If validation fails, ValueError is raised with clear error messages
runner.run()
```

### Validation in Orchestrator

The `Orchestrator` validates LLM-generated YAML:

```python
# LLM generates YAML
yaml_output = engineer.chat(prompt)

# Parse and validate
manifest_dict = yaml.safe_load(yaml_output)
validated_schema = validate_manifest(manifest_dict)

# Only save if valid
with open('manifest.yaml', 'w') as f:
    f.write(yaml_output)
```

---

## Error Messages

### Example: Missing Required Field

**Manifest:**
```yaml
pipeline_name: "test"
agent_type: "generic_rest_api"
source:
  method: "GET"  # Missing URL!
target:
  bucket: "my-bucket"
```

**Error:**
```
❌ Manifest validation failed:
  • source: URL is required for ingestion pipelines
```

### Example: Invalid Pipeline Name

**Manifest:**
```yaml
pipeline_name: "My-Pipeline!"  # Invalid characters
```

**Error:**
```
❌ Manifest validation failed:
  • pipeline_name: String should match pattern '^[a-z0-9_]+$'
```

### Example: Bucket Name Too Short

**Manifest:**
```yaml
target:
  bucket: "ab"  # < 3 chars
```

**Error:**
```
❌ Manifest validation failed:
  • target -> bucket: String should have at least 3 characters
```

---

## Testing

### Unit Tests

**File:** `tests/test_manifest_schemas.py`

Run tests:
```bash
pytest tests/test_manifest_schemas.py -v
```

**Coverage:**
- Pagination validation (4 tests)
- Source configuration (4 tests)
- Target configuration (5 tests)
- AI configuration (4 tests)
- Ingestion manifests (4 tests)
- Transformation manifests (2 tests)
- Real-world manifests (1 test)

### Manual Testing

Test with existing manifest:
```bash
python -c "
from src.schemas.manifest_schemas import validate_manifest
import yaml

manifest = yaml.safe_load(open('manifests/rechtspraak.yaml'))
result = validate_manifest(manifest)
print(f'✅ Valid: {result.pipeline_name}')
"
```

---

## Best Practices

### For Manifest Authors

1. **Use lowercase names** for pipelines, sources, datasets
2. **Follow S3 bucket naming** conventions
3. **Provide meaningful instructions** for AI transformations (> 10 chars)
4. **Set reasonable pagination limits** (≤ 1000)
5. **Test manifests** before committing

### For Developers

1. **Always validate** before execution
2. **Provide clear error messages** to users
3. **Update schemas** when adding new fields
4. **Write tests** for new validation rules
5. **Document** business rules in schema docstrings

---

## Schema Customization

### Adding New Fields

```python
class SourceConfig(BaseModel):
    # Existing fields...
    
    # Add new field
    timeout: Optional[int] = Field(None, gt=0, le=300)
    
    @field_validator('timeout')
    @classmethod
    def validate_timeout(cls, v):
        if v and v > 60:
            logger.warning(f"Timeout {v}s is high, consider reducing")
        return v
```

### Adding New Validation Rules

```python
@field_validator('pipeline_name')
@classmethod
def validate_pipeline_name(cls, v):
    # Existing validation...
    
    # Add new rule
    if 'test' in v and not v.startswith('test_'):
        raise ValueError("Test pipelines must start with 'test_'")
    
    return v
```

---

## Integration Points

### 1. PipelineRunner
**File:** `src/core/runner.py`

Validates manifest on initialization:
```python
def _load_and_validate_manifest(self):
    raw_config = yaml.safe_load(open(self.manifest_path))
    validated_schema = validate_manifest(raw_config)
    self.manifest_config = validated_schema.dict()
```

### 2. Orchestrator
**File:** `src/agents/mas/orchestrator.py`

Validates LLM-generated YAML:
```python
def execute_mission(self, context):
    yaml_output = self.engineer.chat(build_input)
    manifest_dict = yaml.safe_load(yaml_output)
    validated_schema = validate_manifest(manifest_dict)
    return yaml_output
```

---

## Troubleshooting

### Common Issues

**Issue:** `ValidationError: URL is required for ingestion pipelines`  
**Solution:** Add `url` field to `source` configuration

**Issue:** `ValidationError: String should match pattern '^[a-z0-9_]+$'`  
**Solution:** Use only lowercase letters, numbers, and underscores in names

**Issue:** `ValidationError: String should have at least 3 characters`  
**Solution:** Ensure bucket names are at least 3 characters

**Issue:** `ValidationError: offset_param is required for offset pagination`  
**Solution:** Add `offset_param` when using `type: offset` or `offset_limit`

---

## FAQ

**Q: Can I use uppercase in pipeline names?**  
A: No. Pipeline names must be lowercase with underscores only.

**Q: What's the maximum pagination limit?**  
A: 1000 items per request. This prevents excessive API calls.

**Q: Can I add custom fields to manifests?**  
A: No. Pydantic is configured with `extra='forbid'` to reject unknown fields.

**Q: How do I test my manifest?**  
A: Run `python -c "from src.schemas.manifest_schemas import validate_manifest; import yaml; validate_manifest(yaml.safe_load(open('your_manifest.yaml')))"`

**Q: What if validation is too strict?**  
A: Submit a request to relax specific rules. Include business justification.

---

## References

- [Pydantic Documentation](https://docs.pydantic.dev/)
- [AWS S3 Bucket Naming Rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html)
- [YAML Specification](https://yaml.org/spec/)

---

**Last Updated:** 16 februari 2026  
**Version:** 1.0  
**Status:** Production Ready
