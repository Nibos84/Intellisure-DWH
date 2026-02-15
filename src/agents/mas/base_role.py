import logging
from typing import List, Dict, Optional
from src.core.ai_service import ai_service

logger = logging.getLogger(__name__)

class AgentRole:
    """
    Base class for specific agent roles in the MAS.
    Maintains its own context/memory.
    """
    def __init__(self, name: str, role: str, goal: str):
        self.name = name
        self.role = role
        self.goal = goal
        self.history: List[Dict[str, str]] = []
        
        # System Prompt defines the persona
        self.system_prompt = (
            f"You are {name}, a {role}.\n"
            f"Your Goal: {goal}.\n"
            "You are part of an Agile Data Engineering Team.\n"
            "Keep your responses concise, professional, and actionable."
        )
        # Initialize history
        self.history.append({"role": "system", "content": self.system_prompt})

    def chat(self, user_input: str) -> str:
        """
        Sends user input to the agent and returns the response.
        """
        self.history.append({"role": "user", "content": user_input})
        
        response = ai_service.chat(self.history)
        
        self.history.append({"role": "assistant", "content": response})
        return response

    def reset_memory(self):
        self.history = [{"role": "system", "content": self.system_prompt}]
