import logging
import yaml
from src.agents.mas.ingestion_specialist import IngestionSpecialistAgent
from src.agents.mas.transformation_specialist import TransformationSpecialistAgent

logger = logging.getLogger(__name__)

class PipelineRunner:
    def __init__(self, manifest_path: str):
        self.manifest_path = manifest_path
        with open(manifest_path, 'r') as f:
            self.manifest_config = yaml.safe_load(f)
    
    def run(self):
        agent_type = self.manifest_config.get("agent_type")
        pipeline_name = self.manifest_config.get("pipeline_name", "unknown")
        
        logger.info(f"Running pipeline: {pipeline_name} (type: {agent_type})")
        
        if agent_type == "generic_rest_api":
            agent = IngestionSpecialistAgent()
            result = agent.execute(self.manifest_config)
            logger.info(f"Ingestion result: {result}")
        elif agent_type == "generic_ai_transformer":
            agent = TransformationSpecialistAgent()
            result = agent.execute(self.manifest_config)
            logger.info(f"Transformation result: {result}")
        else:
            logger.error(f"Unknown agent type: {agent_type}")

def run_pipeline(manifest_path: str):
    runner = PipelineRunner(manifest_path)
    runner.run()
