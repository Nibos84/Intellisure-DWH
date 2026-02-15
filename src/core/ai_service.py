import logging
import json
from openai import OpenAI
from typing import Any, Dict, List, Optional
from ..core.config import config

logger = logging.getLogger(__name__)

class AIService:
    """
    Manages interactions with OpenAI (LLM).
    Uses the configured model (default: o1-mini) for reasoning.
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
            # For o1-mini/preview, 'system' role might not be fully supported or behaves differently.
            # But standard ChatCompletions API usually accepts it. 
            # If o1-mini is reasoning model, it might have specific constraints.
            # We'll use standard structure first.
            
            # NOTE: o1-preview/mini currently supports 'user' messages primarily for reasoning. 
            # System prompt support varies. We will combine into user prompt if needed.
            # For robust fallback, let's keep it simple.
            
            messages = [
                {"role": "user", "content": f"{system_prompt}\n\n{user_prompt}"}
            ]

            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                # response_format={"type": "json_object"} # o1-mini might not support json_mode yet
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

ai_service = AIService()
