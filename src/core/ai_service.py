import logging
import json
from openai import OpenAI
from typing import Any, Dict, List, Optional
from ..core.config import config

logger = logging.getLogger(__name__)

class AIService:
    """
    Manages interactions with OpenAI (LLM).
    Uses the configured model (default: gpt-3.5-turbo) for reasoning.
    """
    def __init__(self):
        self.client = OpenAI() # Specs API Key from env automatically
        self.model = config.llm_model
        logger.info(f"Initialized AIService with model: {self.model}")

    def transform_data(self, raw_content: str, target_schema: Dict[str, str], instruction: str) -> List[Dict[str, Any]]:
        """
        Uses LLM to extract structured data from raw content based on schema.
        """
        # Construct the prompt
        schema_json = json.dumps(target_schema, indent=2)
        system_prompt = (
            "You are a Data Engineering AI Agent. "
            "Your goal is to extract structured data from raw input (XML, JSON, Text) "
            "and format it EXACTLY according to the provided JSON schema.\n"
            f"Schema:\n{schema_json}\n"
            "Output ONLY valid JSON (a list of objects). No markdown, no explanations."
        )
        
        user_prompt = f"Instruction: {instruction}\n\nRaw Data:\n{raw_content[:20000]}" # Limit context window just in case
        
        try:
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]

            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages
            )
            
            content = response.choices[0].message.content
            
            # Clean up potential markdown code blocks
            if content.startswith("```json"):
                content = content.replace("```json", "").replace("```", "")
            elif content.startswith("```"):
                content = content.replace("```", "")
                
            data = json.loads(content)
            
            # Ensure list format
            if isinstance(data, dict):
                # Maybe wrapped in a key or single object
                if "results" in data:
                    return data["results"]
                if "data" in data:
                    return data["data"]
                return [data]
            return data
            
        except Exception as e:
            logger.error(f"AI Transformation failed: {e}")
            return []

    def generate_config(self, instruction: str, schema: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Generates a configuration object based on an instruction and schema.
        Use this for creating Manifests from natural language.
        """
        schema_json = json.dumps(schema, indent=2)
        system_prompt = (
            "You are a Senior Data Architect. "
            "Your goal is to generate a valid configuration (JSON) based on the user's request. "
            "Use the provided schema as a strict guide.\n"
            f"Schema:\n{schema_json}\n"
            "Output ONLY valid JSON (a single object). No markdown, no explanations."
        )
        
        try:
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": instruction}
            ]

            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages
            )
            
            content = response.choices[0].message.content
            
            if content.startswith("```json"):
                content = content.replace("```json", "").replace("```", "")
            elif content.startswith("```"):
                content = content.replace("```", "")
                
            return json.loads(content)
            
        except Exception as e:
            logger.error(f"AI Config Generation failed: {e}")
            return None

    def generate_plan(self, instruction: str) -> str:
        """
        Generates a natural language implementation plan based on the user request.
        Acts as a consultant/architect.
        """
        system_prompt = (
            "You are a Senior Data Architect. "
            "Your goal is to analyze the user's request and propose a technical implementation plan. "
            "Do NOT generate code or JSON yet. just explain your approach.\n"
            "Structure your response:\n"
            "1. **Understanding**: What is the user asking?\n"
            "2. **Research/Assumptions**: What API/Source will you use? (Simulate knowledge of common APIs like KNMI, Rechtspraak, etc.)\n"
            "3. **Proposed Plan**: How will you ingest and transform this data?\n"
            "4. **Confirmation**: Ask if this plan is acceptable."
        )
        
        try:
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": instruction}
            ]

            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"AI Plan Generation failed: {e}")
            return "Could not generate a plan due to an error."

    def chat(self, messages: List[Dict[str, str]]) -> str:
        """
        Generic chat completion for conversational agents.
        Args:
            messages: List of message dicts (role, content)
        Returns:
            The assistant's response content.
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"AI Chat failed: {e}")
            return f"Error: {str(e)}"

ai_service = AIService()
