import logging
import pandas as pd
import io
import json
from typing import List, Dict, Any
from .base_transformer import BaseTransformer
from ..core.ai_service import ai_service

logger = logging.getLogger(__name__)

class GenericAITransformer(BaseTransformer):
    """
    Transforms ANY raw file into structured data using an LLM.
    Configuration is driven by 'ai_config' in manifest.
    """
    def process_file(self, file_key: str, source_config: dict, target_config: dict):
        ai_config = self.manifest.get("ai_config", {})
        instruction = ai_config.get("instruction")
        target_schema = ai_config.get("schema")
        
        if not instruction or not target_schema:
            logger.error("Missing AI instruction or schema in manifest.")
            return

        # 1. Read Raw Content
        content_bytes = self.s3.read_file(file_key)
        if not content_bytes:
            print(f"DEBUG: Empty content for {file_key}")
            return
            
        try:
            raw_text = content_bytes.decode('utf-8')
        except UnicodeDecodeError:
            # Fallback for binary or weird encoding
            raw_text = str(content_bytes)

        logger.info(f"Asking AI to transform {file_key}...")
        print(f"DEBUG: Transforming {file_key} (len={len(raw_text)})")
        
        # 2. Transform with AI
        structured_data = ai_service.transform_data(raw_text, target_schema, instruction)
        
        if not structured_data:
            logger.warning(f"AI returned no data for {file_key}")
            print(f"DEBUG: AI returned NO DATA for {file_key}")
            return
            
        print(f"DEBUG: AI Success. Got {len(structured_data)} records.")

        # 3. Save to Silver (Parquet)
        self.save_to_silver(structured_data, target_config, file_key)

    def save_to_silver(self, data: List[Dict], target_config: Dict, source_key: str):
        """Saves the extracted data as Parquet to Silver layer."""
        df = pd.DataFrame(data)
        
        # Construct Target Path maintaining Hive partition structure
        # Source: layer=landing/source=rechtspraak/.../year=2026/.../file.xml
        # Target: layer=silver/source=rechtspraak/.../year=2026/.../file.parquet
        
        source_config = self.manifest.get("source", {})
        source_base = source_config.get("path", "")
        target_base = target_config.get("path", "")
        
        # Calculate relative path from source base
        # e.g. /year=2026/month=02/day=15/file.xml
        if source_base and source_key.startswith(source_base):
            relative_path = source_key[len(source_base):].lstrip("/")
        else:
            # Fallback if source key doesn't match base (unlikely but safe)
            relative_path = source_key.split('/')[-1]

        # Swap extension
        new_filename = relative_path.replace('.xml', '.parquet').replace('.json', '.parquet')
        
        full_target_key = f"{target_base}/{new_filename}"
        
        # Write to in-memory buffer
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        self.s3.upload_file(buffer.getvalue(), full_target_key)
        logger.info(f"AI Transformation success: {source_key} -> {full_target_key} ({len(df)} rows)")
