"""
Pydantic schemas for manifest validation.

Validates YAML manifest files for data pipelines to ensure:
- Required fields are present
- Data types are correct
- Values are within acceptable ranges
- Business rules are enforced
"""

import re
import ipaddress
from urllib.parse import urlparse
from typing import Optional, Dict, Any, Literal
from pydantic import BaseModel, Field, field_validator, HttpUrl, ConfigDict
import logging

logger = logging.getLogger(__name__)


class PaginationConfig(BaseModel):
    """Configuration for API pagination."""
    
    model_config = ConfigDict(extra='forbid')
    
    type: Literal["offset", "offset_limit", "cursor", "page", "none"]
    offset_param: Optional[str] = None
    limit_param: Optional[str] = None
    limit_value: Optional[int] = Field(None, gt=0, le=1000)
    max_items: Optional[int] = Field(None, gt=0)
    cursor_param: Optional[str] = None
    page_param: Optional[str] = None
    
    @field_validator('limit_value')
    @classmethod
    def validate_limit_value(cls, v):
        """Ensure limit_value is reasonable."""
        if v is not None and v > 1000:
            logger.warning(f"limit_value {v} is very high, consider reducing for better performance")
        return v
    
    @field_validator('offset_param')
    @classmethod
    def validate_offset_param(cls, v, info):
        """Ensure offset_param is provided for offset pagination."""
        if info.data.get('type') in ['offset', 'offset_limit'] and not v:
            raise ValueError("offset_param is required for offset pagination")
        return v


class SourceConfig(BaseModel):
    """Configuration for data source."""
    
    model_config = ConfigDict(extra='forbid')
    
    # For REST API ingestion
    type: Optional[Literal["rest_api", "file", "database"]] = "rest_api"
    url: Optional[HttpUrl] = None
    method: Optional[Literal["GET", "POST", "PUT"]] = "GET"
    format: Optional[Literal["json", "xml", "csv", "parquet"]] = "json"
    pagination: Optional[PaginationConfig] = None
    params: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    
    # For S3 transformation
    bucket: Optional[str] = Field(None, min_length=3, max_length=63)
    path: Optional[str] = None
    
    @field_validator('bucket')
    @classmethod
    def validate_bucket_name(cls, v):
        """Validate S3 bucket naming conventions."""
        if v is None:
            return v
        
        # S3 bucket naming rules
        if not re.match(r'^[a-z0-9][a-z0-9-]*[a-z0-9]$', v):
            raise ValueError(
                "Bucket name must start and end with lowercase letter or number, "
                "and contain only lowercase letters, numbers, and hyphens"
            )
        
        if '..' in v or '.-' in v or '-.' in v:
            raise ValueError("Bucket name cannot contain consecutive periods or period-hyphen combinations")
        
        return v
    
    @field_validator('path')
    @classmethod
    def validate_path(cls, v):
        """Validate S3 path format."""
        if v is None:
            return v
        
        # Ensure path doesn't start with /
        if v.startswith('/'):
            raise ValueError("S3 path should not start with /")
        
        return v
    
    @field_validator('url')
    @classmethod
    def validate_public_url(cls, v):
        """Ensure URL points to public internet, not private networks."""
        if not v:
            return v
        
        parsed = urlparse(str(v))
        hostname = parsed.hostname
        
        if not hostname:
            raise ValueError("URL must have a valid hostname")
        
        # Block localhost variants
        if hostname.lower() in ['localhost', '127.0.0.1', '::1', '0.0.0.0']:
            raise ValueError(
                f"Localhost URLs not allowed: {hostname}. "
                f"This platform is for public data sources only."
            )
        
        # Check if hostname is an IP address
        try:
            ip = ipaddress.ip_address(hostname)
            
            # Block private IP ranges
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                raise ValueError(
                    f"Private/reserved IP address not allowed: {hostname}. "
                    f"This platform is for public data sources only. "
                    f"Use public domain names instead."
                )
        except ValueError as e:
            # If it's our validation error, re-raise it
            if "not allowed" in str(e):
                raise
            # Otherwise it's not an IP address, it's a hostname - that's fine
            pass
        
        # Additional check: block common private hostnames
        private_hostname_patterns = [
            'internal',
            'corp',
            'intranet',
            '.local',
            '.lan',
        ]
        
        hostname_lower = hostname.lower()
        for pattern in private_hostname_patterns:
            if pattern in hostname_lower:
                raise ValueError(
                    f"Private hostname pattern detected: {hostname}. "
                    f"This platform is for public data sources only."
                )
        
        return v


class TargetConfig(BaseModel):
    """Configuration for data target (S3)."""
    
    model_config = ConfigDict(extra='forbid')
    
    bucket: str = Field(..., min_length=3, max_length=63)
    layer: Optional[Literal["landing", "silver", "gold"]] = "landing"
    source: Optional[str] = Field(None, pattern=r'^[a-z0-9_]+$')
    dataset: Optional[str] = Field(None, pattern=r'^[a-z0-9_]+$')
    path: Optional[str] = None
    
    @field_validator('bucket')
    @classmethod
    def validate_bucket_name(cls, v):
        """Validate S3 bucket naming conventions."""
        # S3 bucket naming rules
        if not re.match(r'^[a-z0-9][a-z0-9-]*[a-z0-9]$', v):
            raise ValueError(
                "Bucket name must start and end with lowercase letter or number, "
                "and contain only lowercase letters, numbers, and hyphens"
            )
        
        if '..' in v or '.-' in v or '-.' in v:
            raise ValueError("Bucket name cannot contain consecutive periods or period-hyphen combinations")
        
        return v
    
    @field_validator('source')
    @classmethod
    def validate_source_name(cls, v):
        """Validate source naming convention."""
        if v and not re.match(r'^[a-z0-9_]+$', v):
            raise ValueError("Source name must contain only lowercase letters, numbers, and underscores")
        return v
    
    @field_validator('dataset')
    @classmethod
    def validate_dataset_name(cls, v):
        """Validate dataset naming convention."""
        if v and not re.match(r'^[a-z0-9_]+$', v):
            raise ValueError("Dataset name must contain only lowercase letters, numbers, and underscores")
        return v
    
    @field_validator('path')
    @classmethod
    def validate_path(cls, v):
        """Validate S3 path format."""
        if v is None:
            return v
        
        # Ensure path doesn't start with /
        if v.startswith('/'):
            raise ValueError("S3 path should not start with /")
        
        return v


class AIConfig(BaseModel):
    """Configuration for AI-powered transformation."""
    
    model_config = ConfigDict(extra='forbid')
    
    instruction: str = Field(..., min_length=10, max_length=5000)
    schema: Dict[str, str] = Field(...)
    
    @field_validator('instruction')
    @classmethod
    def validate_instruction(cls, v):
        """Ensure instruction is meaningful."""
        if not v.strip():
            raise ValueError("Instruction cannot be empty or whitespace only")
        return v.strip()
    
    @field_validator('schema')
    @classmethod
    def validate_schema(cls, v):
        """Validate schema field types."""
        if not v:
            raise ValueError("Schema cannot be empty")
            
        valid_types = {
            'str', 'int', 'float', 'bool', 
            'datetime64[ns]', 'object',
            'string', 'integer', 'number', 'boolean'
        }
        
        for field_name, field_type in v.items():
            if field_type not in valid_types:
                logger.warning(f"Uncommon field type '{field_type}' for field '{field_name}'")
        
        return v


class IngestionManifestSchema(BaseModel):
    """Schema for data ingestion manifests."""
    
    model_config = ConfigDict(extra='forbid')
    
    pipeline_name: str = Field(..., pattern=r'^[a-z0-9_]+$', min_length=3, max_length=100)
    agent_type: Literal["generic_rest_api"]
    source: SourceConfig
    target: TargetConfig
    
    @field_validator('pipeline_name')
    @classmethod
    def validate_pipeline_name(cls, v):
        """Validate pipeline naming convention."""
        if not re.match(r'^[a-z0-9_]+$', v):
            raise ValueError(
                "Pipeline name must contain only lowercase letters, numbers, and underscores"
            )
        
        if v.startswith('_') or v.endswith('_'):
            raise ValueError("Pipeline name cannot start or end with underscore")
        
        if '__' in v:
            raise ValueError("Pipeline name cannot contain consecutive underscores")
        
        return v
    
    @field_validator('source')
    @classmethod
    def validate_source_for_ingestion(cls, v):
        """Ensure source has required fields for ingestion."""
        if not v.url:
            raise ValueError("URL is required for ingestion pipelines")
        
        if v.type != "rest_api":
            raise ValueError(f"Source type '{v.type}' is not supported for ingestion (use 'rest_api')")
        
        return v
    
    @field_validator('target')
    @classmethod
    def validate_target_for_ingestion(cls, v):
        """Ensure target has required fields for ingestion."""
        if not v.source:
            raise ValueError("Target source name is required for ingestion")
        
        if not v.dataset:
            raise ValueError("Target dataset name is required for ingestion")
        
        return v


class TransformationManifestSchema(BaseModel):
    """Schema for data transformation manifests."""
    
    model_config = ConfigDict(extra='forbid')
    
    pipeline_name: str = Field(..., pattern=r'^[a-z0-9_]+$', min_length=3, max_length=100)
    agent_type: Literal["generic_ai_transformer"]
    source: SourceConfig
    target: TargetConfig
    ai_config: AIConfig
    
    @field_validator('pipeline_name')
    @classmethod
    def validate_pipeline_name(cls, v):
        """Validate pipeline naming convention."""
        if not re.match(r'^[a-z0-9_]+$', v):
            raise ValueError(
                "Pipeline name must contain only lowercase letters, numbers, and underscores"
            )
        
        if v.startswith('_') or v.endswith('_'):
            raise ValueError("Pipeline name cannot start or end with underscore")
        
        if '__' in v:
            raise ValueError("Pipeline name cannot contain consecutive underscores")
        
        return v
    
    @field_validator('source')
    @classmethod
    def validate_source_for_transformation(cls, v):
        """Ensure source has required fields for transformation."""
        if not v.path:
            raise ValueError("Source path is required for transformation pipelines")
        
        if not v.bucket:
            raise ValueError("Source bucket is required for transformation pipelines")
        
        return v
    
    @field_validator('target')
    @classmethod
    def validate_target_for_transformation(cls, v):
        """Ensure target has required fields for transformation."""
        if not v.path:
            raise ValueError("Target path is required for transformation")
        
        return v


def validate_manifest(config: dict) -> BaseModel:
    """
    Validate a manifest configuration and return the validated schema.
    
    Args:
        config: Raw manifest dictionary from YAML
        
    Returns:
        Validated Pydantic model
        
    Raises:
        ValueError: If agent_type is unknown
        ValidationError: If validation fails
    """
    agent_type = config.get("agent_type")
    
    if agent_type == "generic_rest_api":
        return IngestionManifestSchema(**config)
    elif agent_type == "generic_ai_transformer":
        return TransformationManifestSchema(**config)
    else:
        raise ValueError(
            f"Unknown agent_type: '{agent_type}'. "
            f"Must be 'generic_rest_api' or 'generic_ai_transformer'"
        )
