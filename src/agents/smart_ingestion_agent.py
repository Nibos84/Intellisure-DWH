import requests
import json
import logging
from typing import Dict, Any, List
from datetime import datetime
from .base_agent import BaseAgent

logger = logging.getLogger(__name__)

class SmartIngestionAgent(BaseAgent):
    """
    Generic Data Ingestion Agent for REST APIs.
    Configured via manifest. supports standard pagination.
    """
    def run(self):
        source_config = self.manifest.get("source", {})
        target_config = self.manifest.get("target", {})
        
        url = source_config.get("url")
        method = source_config.get("method", "GET")
        fmt = source_config.get("format", "json").lower()
        base_params = source_config.get("params", {})
        pagination = source_config.get("pagination", {})
        
        if not url:
            logger.error("No URL provided in manifest source config.")
            return

        logger.info(f"Starting ingestion from {url} [Format: {fmt}]")
        
        # Initialize pagination
        offset = 0
        limit = pagination.get("limit_value", 100)
        items_fetched = 0
        max_items = pagination.get("max_items", 1000)

        while True:
            # Merge base params with pagination params
            params = base_params.copy()
            params.update({
                pagination.get("offset_param", "from"): offset,
                pagination.get("limit_param", "size"): limit
            })
            
            try:
                response = requests.request(method, url, params=params)
                response.raise_for_status()
                
                if fmt == "json":
                    data = response.json()
                    items = data if isinstance(data, list) else data.get("results", [])
                    if not items:
                        logger.info("No more items to fetch (JSON empty).")
                        break
                    
                    self._upload_batch(items, target_config, offset, fmt="json")
                    count = len(items)
                
                else:
                    # XML / Text / Binary
                    # For Atom feeds, it's hard to know if empty without parsing.
                    # Simple heuristic: Check for <entry> tag if XML
                    content = response.text
                    if fmt == "xml" and "<entry>" not in content:
                        logger.info("No more items to fetch (XML no <entry>).")
                        break
                    
                    # Upload raw content
                    self._upload_batch(content, target_config, offset, fmt=fmt)
                    count = limit  # Approx, we don't know exact count without parsing
                    
                items_fetched += count
                offset += limit # For XML/Offset usually we step by limit. 
                # Note: If JSON returned fewer items than limit, we should stop? 
                # logic for JSON is handled by empty list check above.
                
                logger.info(f"Fetched batch at offset {offset}. Total est: {items_fetched}")
                
                if items_fetched >= max_items:
                    logger.info("Reached maximum items limit.")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching data: {e}")
                break

    def _upload_batch(self, data: Any, target_config: Dict, batch_id: int, fmt: str):
        """Uploads a batch of items to S3."""
        bucket = target_config.get("bucket")
        # Support both 'path' (legacy) and Hive components
        layer = target_config.get("layer", "landing")
        source = target_config.get("source", "unknown_source")
        dataset = target_config.get("dataset", "unknown_dataset")
        
        timestamp = datetime.now()
        ts_str = timestamp.strftime("%Y%m%d%H%M%S")
        
        # Hive Partitioning: layer=X/source=Y/dataset=Z/year=YYYY/month=MM/day=DD
        date_partition = f"year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}"
        
        if "path" in target_config:
            # Legacy/Manual Override
            base_path = target_config["path"]
        else:
            # Automatic Hive Construction
            base_path = f"layer={layer}/source={source}/dataset={dataset}/{date_partition}"

        if fmt == "json":
            filename = f"{base_path}/batch_{batch_id}_{ts_str}.json"
            content = json.dumps(data, indent=2)
        else:
            ext = fmt if fmt != "text" else "txt"
            filename = f"{base_path}/batch_{batch_id}_{ts_str}.{ext}"
            content = data
        
        self.s3.upload_file(content, filename)
