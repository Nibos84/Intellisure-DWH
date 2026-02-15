import logging
import json
from typing import Dict, Any, List, Optional
from src.agents.mas.base_role import AgentRole
from src.core.s3_manager import s3_manager
from src.core.ai_service import ai_service

logger = logging.getLogger(__name__)

class TransformationSpecialistAgent(AgentRole):
    """
    Reasoning-based data transformation agent.
    Uses LLM to understand data patterns and transform intelligently.
    """
    def __init__(self):
        super().__init__(
            name="Transformation Specialist",
            role="Data Transformation Expert",
            goal="Transform raw data into structured, clean formats using intelligent reasoning and schema inference."
        )
        self.system_prompt += (
            "\nAdditional Instructions:\n"
            "- You are responsible for transforming raw data (JSON, XML, CSV) into clean, structured formats.\n"
            "- Analyze data quality and adapt your transformation strategy.\n"
            "- Infer schemas when not explicitly provided.\n"
            "- Handle missing, corrupt, or inconsistent data intelligently.\n"
            "- Always explain your reasoning and the transformations you're applying."
        )
        
    def execute(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the transformation pipeline based on the manifest.
        Uses LLM reasoning to adapt transformation strategy.
        """
        pipeline_name = manifest.get("pipeline_name", "unknown")
        source_config = manifest.get("source", {})
        target_config = manifest.get("target", {})
        ai_config = manifest.get("ai_config", {})
        
        logger.info(f"[{self.name}] Starting transformation: {pipeline_name}")
        
        # Get source files
        source_path = source_config.get("path", "")
        files = s3_manager.list_files(source_path)
        
        if not files:
            logger.warning(f"[{self.name}] No files found in {source_path}")
            return {"status": "no_data"}
        
        # Ask LLM for transformation strategy
        strategy_prompt = (
            f"I need to transform data with this goal:\n"
            f"Instruction: {ai_config.get('instruction', 'Transform to structured format')}\n"
            f"Target Schema: {ai_config.get('schema', 'Not specified - infer from data')}\n"
            f"Number of files: {len(files)}\n\n"
            f"What transformation strategy should I use? What quality checks should I perform?"
        )
        
        strategy = self.chat(strategy_prompt)
        logger.info(f"[{self.name}] Strategy: {strategy}")
        
        # Process files
        processed_count = 0
        for file_key in files[:3]:  # Limit for demo
            try:
                self._transform_file(file_key, ai_config, target_config)
                processed_count += 1
            except Exception as e:
                # Ask LLM for error handling
                error_guidance = self.chat(
                    f"Error transforming {file_key}: {str(e)}\n"
                    f"Should I skip this file or try a different approach?"
                )
                logger.error(f"[{self.name}] Error guidance: {error_guidance}")
        
        return {"status": "success", "processed": processed_count, "total": len(files)}
    
    def _transform_file(self, file_key: str, ai_config: Dict, target_config: Dict):
        """Transform a single file using LLM reasoning."""
        # Read source data
        content_bytes = s3_manager.read_file(file_key)
        if not content_bytes:
            return
        
        try:
            raw_text = content_bytes.decode('utf-8')
        except UnicodeDecodeError:
            raw_text = str(content_bytes)
        
        logger.info(f"[{self.name}] Transforming {file_key} (size: {len(raw_text)} chars)")
        
        # Use AI to transform
        instruction = ai_config.get('instruction', 'Extract structured data')
        target_schema = ai_config.get('schema', {})
        
        # Ask LLM to transform the data
        transform_prompt = (
            f"Transform this data according to the instruction.\n\n"
            f"Instruction: {instruction}\n"
            f"Target Schema: {json.dumps(target_schema, indent=2)}\n\n"
            f"Raw Data (first 5000 chars):\n{raw_text[:5000]}\n\n"
            f"Return ONLY a valid JSON array of objects matching the schema."
        )
        
        transformed_data = ai_service.transform_data(
            raw_text[:10000],  # Limit context
            target_schema,
            instruction
        )
        
        if not transformed_data:
            logger.warning(f"[{self.name}] No data extracted from {file_key}")
            return
        
        # Save transformed data
        target_path = target_config.get("path", "layer=silver")
        output_key = file_key.replace("layer=landing", target_path).replace(".xml", ".json").replace(".csv", ".json")
        
        output_content = json.dumps(transformed_data, indent=2).encode('utf-8')
        s3_manager.write_file(output_key, output_content)
        
        logger.info(f"[{self.name}] Saved {len(transformed_data)} records to {output_key}")
