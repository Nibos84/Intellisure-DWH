import logging
import re
from typing import Dict, Any, Optional
from src.agents.mas.roles import ResearcherAgent, ArchitectAgent, EngineerAgent

logger = logging.getLogger(__name__)

class Orchestrator:
    """
    Coordinates the Agile AI Team workflow.
    """
    def __init__(self):
        self.researcher = ResearcherAgent()
        self.architect = ArchitectAgent()
        self.engineer = EngineerAgent()
        
    def start_mission(self, mission: str) -> Dict[str, str]:
        """
        Phase 1: Research & Planning.
        Returns a dictionary with research findings and architectural plan.
        """
        logger.info(f"Starting mission: {mission}")
        
        # 1. Research
        logger.info("Engaging Researcher...")
        research_output = self.researcher.chat(f"Analyze this request: {mission}")
        
        # 2. Architect Plan
        logger.info("Engaging Architect...")
        plan_input = (
            f"Mission: {mission}\n"
            f"Research Findings: {research_output}\n"
            "Based on this research, design a data pipeline strategy."
        )
        plan_output = self.architect.chat(plan_input)
        
        return {
            "research": research_output,
            "plan": plan_output,
            "mission": mission # Keep context
        }

    def execute_mission(self, context: Dict[str, str]) -> Optional[str]:
        """
        Phase 2: Execution (Manifest Generation).
        Takes the approved plan and generates the YAML manifest.
        Returns the YAML content.
        """
        logger.info("Engaging Engineer...")
        
        # Provide concrete templates
        yaml_templates = """
YAML TEMPLATE FOR INGESTION:
```yaml
pipeline_name: "source_ingestion"
agent_type: "generic_rest_api"
source:
  type: rest_api
  url: "https://api.example.com/data"
  method: GET
  format: json
  pagination:
    type: offset
    offset_param: skip
    limit_param: limit
target:
  bucket: "splendid-bethe"
  layer: landing
  source: "example"
  dataset: "data"
```

YAML TEMPLATE FOR TRANSFORMATION (PYTHON PANDAS GENERATION):
```yaml
pipeline_name: "source_silver_ai"
agent_type: "generic_ai_transformer"
source:
  bucket: "splendid-bethe"
  path: "layer=landing/source=example/dataset=data"
target:
  bucket: "splendid-bethe"
  path: "layer=silver/source=example/dataset=data"
ai_config:
  instruction: "Clean data, standardize dates to YYYY-MM-DD, drop duplicates"
  schema:
    field1: str
    field2: float
    date_field: datetime64[ns]
```
"""
        
        build_input = (
            f"Mission: {context['mission']}\n"
            f"Architect's Plan: {context['plan']}\n\n"
            f"{yaml_templates}\n\n"
            "Based on the mission and plan above, adapt one of these templates and write the complete Manifest YAML. "
            "Output ONLY the YAML code block, nothing else."
        )
        
        yaml_output = self.engineer.chat(build_input)
        
        # Extract YAML from markdown code block
        match = re.search(r"```(?:yaml)?\n(.*?)\n```", yaml_output, re.DOTALL)
        if match:
            yaml_output = match.group(1)
        else:
             # Fallback: remove any single backticks or leading/trailing whitespace
             yaml_output = yaml_output.strip().strip('`')
             
        return yaml_output.strip()

orchestrator = Orchestrator()
