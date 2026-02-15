import logging
import requests
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from src.agents.mas.base_role import AgentRole
from src.core.s3_manager import s3_manager

logger = logging.getLogger(__name__)

class IngestionSpecialistAgent(AgentRole):
    """
    Reasoning-based data ingestion agent.
    Uses LLM to adapt to API quirks and handle errors intelligently.
    """
    def __init__(self):
        super().__init__(
            name="Ingestion Specialist",
            role="Data Ingestion Expert",
            goal="Fetch data from external sources and store it in the data lake with intelligent error handling and adaptation."
        )
        self.system_prompt += (
            "\nAdditional Instructions:\n"
            "- You are responsible for fetching data from REST APIs.\n"
            "- Analyze API responses and adapt your strategy (pagination, rate limiting, error recovery).\n"
            "- When you encounter errors, reason about the cause and suggest solutions.\n"
            "- You have access to tools: fetch_url, save_to_s3, analyze_response.\n"
            "- Always respond with your reasoning and the action you're taking."
        )
        
    def execute(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the ingestion pipeline based on the manifest.
        Uses LLM reasoning to adapt to API behavior.
        """
        pipeline_name = manifest.get("pipeline_name", "unknown")
        source_config = manifest.get("source", {})
        target_config = manifest.get("target", {})
        
        logger.info(f"[{self.name}] Starting ingestion: {pipeline_name}")
        
        # Ask LLM to analyze the task
        task_description = (
            f"I need to ingest data from this source:\n"
            f"URL: {source_config.get('url')}\n"
            f"Method: {source_config.get('method', 'GET')}\n"
            f"Format: {source_config.get('format', 'json')}\n"
            f"Pagination: {source_config.get('pagination', {})}\n\n"
            f"What strategy should I use? What potential issues should I watch for?"
        )
        
        strategy = self.chat(task_description)
        logger.info(f"[{self.name}] Strategy: {strategy}")
        
        # Execute ingestion with LLM-guided approach
        try:
            data = self._fetch_data(source_config, strategy)
            self._save_data(data, target_config, pipeline_name)
            
            return {"status": "success", "records": len(data)}
        except Exception as e:
            # Ask LLM for error recovery
            error_analysis = self.chat(
                f"I encountered this error: {str(e)}\n"
                f"What should I do? Should I retry? Adjust parameters?"
            )
            logger.error(f"[{self.name}] Error analysis: {error_analysis}")
            return {"status": "failed", "error": str(e), "analysis": error_analysis}
    
    def _fetch_data(self, source_config: Dict, strategy: str) -> List[Dict]:
        """Fetch data from the source."""
        url = source_config.get("url")
        method = source_config.get("method", "GET")
        
        response = requests.request(method, url)
        response.raise_for_status()
        
        data = response.json()
        
        # Handle different response structures
        if isinstance(data, dict):
            # Ask LLM which key contains the actual data
            analysis = self.chat(
                f"The API returned a dict with these keys: {list(data.keys())}.\n"
                f"Which key likely contains the data records?"
            )
            logger.info(f"[{self.name}] Data extraction guidance: {analysis}")
            
            # Simple heuristic: look for common keys
            for key in ['data', 'results', 'items', 'records']:
                if key in data:
                    data = data[key]
                    break
        
        return data if isinstance(data, list) else [data]
    
    def _save_data(self, data: List[Dict], target_config: Dict, pipeline_name: str):
        """Save data to S3."""
        bucket = target_config.get("bucket")
        layer = target_config.get("layer", "landing")
        source = target_config.get("source", "unknown")
        dataset = target_config.get("dataset", "data")
        
        # Hive-style partitioning
        now = datetime.now()
        partition_path = (
            f"layer={layer}/source={source}/dataset={dataset}/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
        )
        
        filename = f"batch_{now.strftime('%Y%m%d%H%M%S')}.json"
        s3_key = f"{partition_path}/{filename}"
        
        content = json.dumps(data, indent=2).encode('utf-8')
        s3_manager.write_file(s3_key, content)
        
        logger.info(f"[{self.name}] Saved {len(data)} records to {s3_key}")
