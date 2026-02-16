import logging
import yaml
from pydantic import ValidationError
from src.agents.mas.ingestion_specialist import IngestionSpecialistAgent
from src.agents.mas.transformation_specialist import TransformationSpecialistAgent
from src.schemas.manifest_schemas import validate_manifest

logger = logging.getLogger(__name__)


class PipelineRunner:
    """
    Executes data pipelines based on manifest configuration.
    
    Validates manifest before execution to ensure:
    - Required fields are present
    - Data types are correct
    - Values are within acceptable ranges
    - Business rules are enforced
    """
    
    def __init__(self, manifest_path: str):
        self.manifest_path = manifest_path
        self.manifest_config = None
        self._load_and_validate_manifest()
    
    def _load_and_validate_manifest(self):
        """Load and validate manifest from YAML file."""
        try:
            # Load raw YAML
            with open(self.manifest_path, 'r') as f:
                raw_config = yaml.safe_load(f)
            
            logger.info(f"Loaded manifest from {self.manifest_path}")
            
            # Validate with Pydantic
            validated_schema = validate_manifest(raw_config)
            
            # Convert back to dict for compatibility
            self.manifest_config = validated_schema.dict()
            
            logger.info(f"✅ Manifest validation passed for pipeline: {self.manifest_config.get('pipeline_name')}")
            
        except FileNotFoundError:
            logger.error(f"Manifest file not found: {self.manifest_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Invalid YAML in manifest: {e}")
            raise ValueError(f"Invalid YAML syntax: {e}")
        except ValidationError as e:
            logger.error(f"❌ Manifest validation failed:")
            
            # Format validation errors for better readability
            for error in e.errors():
                field = " -> ".join(str(loc) for loc in error['loc'])
                message = error['msg']
                logger.error(f"  • {field}: {message}")
            
            raise ValueError(f"Manifest validation failed. See logs for details.") from e
        except ValueError as e:
            logger.error(f"Validation error: {e}")
            raise
    
    def run(self):
        """Execute the pipeline based on manifest configuration."""
        if not self.manifest_config:
            raise RuntimeError("Manifest not loaded. Call _load_and_validate_manifest first.")
        
        agent_type = self.manifest_config.get("agent_type")
        pipeline_name = self.manifest_config.get("pipeline_name", "unknown")
        
        logger.info(f"🚀 Starting pipeline: {pipeline_name} (type: {agent_type})")
        
        if agent_type == "generic_rest_api":
            agent = IngestionSpecialistAgent()
            result = agent.execute(self.manifest_config)
            logger.info(f"✅ Ingestion completed: {result}")
        elif agent_type == "generic_ai_transformer":
            agent = TransformationSpecialistAgent()
            result = agent.execute(self.manifest_config)
            logger.info(f"✅ Transformation completed: {result}")
        else:
            logger.error(f"Unknown agent type: {agent_type}")
            raise ValueError(f"Unknown agent type: {agent_type}")


def run_pipeline(manifest_path: str):
    """
    Convenience function to run a pipeline from a manifest file.
    
    Args:
        manifest_path: Path to the YAML manifest file
    """
    runner = PipelineRunner(manifest_path)
    runner.run()
