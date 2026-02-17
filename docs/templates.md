# Script Templates

This directory contains reusable script templates that guide LLM code generation.

## Purpose

Templates reduce LLM hallucinations and ensure consistent code patterns by providing reference implementations for common tasks.

## Available Templates

### Ingestion Templates

#### `ingestion/rest_api_pagination.py`
- **Use Case**: Fetching data from paginated REST APIs
- **Features**:
  - Page-based pagination
  - Retry logic with exponential backoff
  - Multiple response format handling
  - Error handling and progress logging
  - S3 upload using presigned URLs

### Transformation Templates

#### `transformation/pandas_transformation.py`
- **Use Case**: Data transformation with Pandas
- **Features**:
  - S3 download using presigned URLs
  - Pandas DataFrame transformations
  - Schema type validation
  - Type conversions (int, datetime, etc.)
  - S3 upload with proper formatting

## How Templates Are Used

Templates are referenced in LLM prompts to guide code generation:

1. **Ingestion Specialist**: References pagination template for API ingestion
2. **Transformation Specialist**: References transformation template for data processing

## Customization

You can add new templates by:

1. Creating a new `.py` file in the appropriate directory
2. Following the existing template structure
3. Updating the LLM prompts in the specialist agents to reference the new template

## Best Practices

- Keep templates focused on a single pattern
- Include comprehensive error handling
- Add detailed comments explaining the pattern
- Use environment variables for configuration
- Follow security best practices (no credentials in code)
