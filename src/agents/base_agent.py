import logging
from typing import Any, Dict
from ..core.config import config
from ..core.s3_manager import s3_manager

logger = logging.getLogger(__name__)

class BaseAgent:
    """
    Base class for all Data Engineering Agents.
    Provides common functionality like logging configuration and S3 access.
    """
    def __init__(self, name: str, manifest: Dict[str, Any]):
        self.name = name
        self.manifest = manifest
        self.s3 = s3_manager
        logger.info(f"Agent '{self.name}' initialized.")

    def run(self):
        """Main execution method to be overridden by subclasses."""
        raise NotImplementedError("Subclasses must implement run()")

    def log(self, message: str, level: int = logging.INFO):
        logger.log(level, f"[{self.name}] {message}")
