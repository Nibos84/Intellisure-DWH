from src.agents.mas.base_role import AgentRole

class ResearcherAgent(AgentRole):
    def __init__(self):
        super().__init__(
            name="Researcher",
            role="Data Researcher",
            goal="Analyze data sources, identify API structures, formats, and access methods."
        )
        self.system_prompt += (
            "\nAdditional Instructions:\n"
            "- If a URL is provided, simulate checking its documentation.\n"
            "- Identify the format (JSON, XML), method (GET), and pagination strategy (Offset, Cursor).\n"
            "- Be precise and factual. Start your response with 'Research Findings:'."
        )

class ArchitectAgent(AgentRole):
    def __init__(self):
        super().__init__(
            name="Architect",
            role="Solution Architect",
            goal="Design a robust data pipeline strategy based on research findings."
        )
        self.system_prompt += (
            "\nAdditional Instructions:\n"
            "- Based on the Researcher's findings, propose a pipeline design.\n"
            "- Define the Source -> Landing -> Silver flow.\n"
            "- Suggest naming conventions for S3 paths (layer=landing/source=...).\n"
            "- Start your response with 'Proposed Plan:'."
        )

class EngineerAgent(AgentRole):
    def __init__(self):
        super().__init__(
            name="Engineer",
            role="DevOps Engineer",
            goal="Translate the architectural plan into executable Manifest YAML configurations."
        )
        self.system_prompt += (
            "\nAdditional Instructions:\n"
            "- You are the only one allowed to write code/YAML.\n"
            "- Output valid YAML for data ingestion or transformation pipelines.\n"
            "- Use these EXACT structures:\n\n"
            "FOR INGESTION (REST API):\n"
            "```yaml\n"
            "pipeline_name: \"source_ingestion\"\n"
            "agent_type: \"generic_rest_api\"\n"
            "source:\n"
            "  type: rest_api\n"
            "  url: \"https://api.example.com/data\"\n"
            "  method: GET\n"
            "  format: json\n"
            "  pagination:\n"
            "    type: offset\n"
            "    offset_param: skip\n"
            "    limit_param: limit\n"
            "target:\n"
            "  bucket: \"splendid-bethe\"\n"
            "  layer: landing\n"
            "  source: \"example\"\n"
            "  dataset: \"data\"\n"
            "```\n\n"
            "FOR TRANSFORMATION (AI):\n"
            "```yaml\n"
            "pipeline_name: \"source_silver_ai\"\n"
            "agent_type: \"generic_ai_transformer\"\n"
            "source:\n"
            "  bucket: \"splendid-bethe\"\n"
            "  path: \"layer=landing/source=example/dataset=data\"\n"
            "target:\n"
            "  bucket: \"splendid-bethe\"\n"
            "  path: \"layer=silver/source=example/dataset=data\"\n"
            "ai_config:\n"
            "  instruction: \"Extract fields X, Y, Z\"\n"
            "  schema:\n"
            "    field1: string\n"
            "    field2: number\n"
            "```\n\n"
            "- Adapt these templates based on the Architect's plan.\n"
            "- Output ONLY the YAML within a ```yaml code block.\n"
            "- Do NOT add explanations or commentary outside the code block."
        )
