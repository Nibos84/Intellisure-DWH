from typing import List, Any
import logging
from .base_agent import BaseAgent

logger = logging.getLogger(__name__)

class BaseTransformer(BaseAgent):
    """
    Base class for Transformation Agents.
    Reads from Source Layer -> Transforms -> Writes to Target Layer.
    """
    def run(self):
        source_config = self.manifest.get("source", {})
        target_config = self.manifest.get("target", {})
        
        bucket = source_config.get("bucket", self.s3.bucket_name)
        prefix = source_config.get("path", "")
        
        logger.info(f"Scanning for files in {bucket}/{prefix}...")
        print(f"DEBUG: Scanning bucket={self.s3.bucket_name} prefix={prefix}")
        
        files = self.s3.list_files(prefix)
        logger.info(f"Found {len(files)} files to process.")
        print(f"DEBUG: Found {len(files)} files.")
        
        processed_count = 0
        for file_key in files:
            try:
                self.process_file(file_key, source_config, target_config)
                processed_count += 1
            except Exception as e:
                logger.error(f"Failed to process {file_key}: {e}")
                
        logger.info(f"Transformation complete. Processed {processed_count}/{len(files)} files.")

    def process_file(self, file_key: str, source_config: dict, target_config: dict):
        """
        To be implemented by subclasses.
        Should read file -> transform -> write.
        """
        raise NotImplementedError("Subclasses must implement process_file")
