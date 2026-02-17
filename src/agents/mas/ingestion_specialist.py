import logging
import subprocess
import os
import sys
import re
import ast
from typing import Dict, Any, Optional
from src.agents.mas.base_role import AgentRole
from src.core.config import config
from src.security.code_validator import CodeValidator
from src.security.s3_credential_service import S3CredentialService
from src.utils.execution import time_limit, TimeoutException
from src.utils.script_cache import get_script_cache

logger = logging.getLogger(__name__)

class IngestionSpecialistAgent(AgentRole):
    """
    Code-generating data ingestion agent.
    Generates and executes Python scripts to ingest data from external sources.
    """
    def __init__(self):
        super().__init__(
            name="Ingestion Specialist",
            role="Data Ingestion Expert",
            goal="Generate and execute robust Python scripts to ingest data from external sources into the data lake."
        )
        self.validator = CodeValidator()
        self.max_retries = 3
        self.system_prompt = (
            "You are an expert Data Engineer specializing in building robust data ingestion pipelines.\n"
            "Your goal is to write Python scripts that fetch data from APIs and store it in S3.\n"
            "The scripts must be production-ready: handle pagination, rate limiting, retries, and errors gracefully.\n"
            "You must output ONLY the Python code within a ```python block.\n"
            "Do NOT provide explanations or commentary outside the code block.\n"
            "IMPORTANT: Only use safe imports (pandas, boto3, requests, json, datetime). "
            "Do NOT use os.system, subprocess, eval, exec, or other dangerous functions."
        )
        
    def execute(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the ingestion pipeline by generating and running a script.
        """
        pipeline_name = manifest.get("pipeline_name", "unknown")
        logger.info(f"[{self.name}] Starting ingestion for pipeline: {pipeline_name}")
        
        # 1. Check cache first
        cache = get_script_cache()
        cached_script = cache.get(manifest)
        
        if cached_script:
            logger.info(f"[{self.name}] Using cached script for {pipeline_name}")
            script_content = cached_script
        else:
            # 2. Generate and validate the ingestion script (with retries)
            script_content = self._generate_and_validate_script(manifest)
            if not script_content:
                return {"status": "failed", "error": "Failed to generate valid script after retries"}
            
            # Store in cache for future use
            cache.set(manifest, script_content)
            logger.info(f"[{self.name}] Script generated and cached for {pipeline_name}")
            
        # 3. Save script to file with unique name to prevent race conditions
        import uuid
        script_path = f"ingest_{pipeline_name}_{uuid.uuid4().hex[:8]}.py"
        with open(script_path, "w") as f:
            f.write(script_content)

        logger.info(f"[{self.name}] Generated script saved to {script_path}")
        
        # Determine target S3 path for dry-run logging
        target_manifest = manifest.get("target", {})
        bucket_name = target_manifest.get("bucket", config.bucket_name)
        layer_name = target_manifest.get("layer", "landing")
        source_name = target_manifest.get("source", "unknown")
        dataset_name = target_manifest.get("dataset", "data")
        s3_key_dry_run = f"{layer_name}/{source_name}/{dataset_name}/data.json"

        # Check dry-run mode
        if config.dry_run:
            logger.info(f"[DRY-RUN] Would execute script: {script_path}")
            logger.info(f"[DRY-RUN] Script validated successfully (AST + CodeValidator)")
            logger.info(f"[DRY-RUN] Target: s3://{bucket_name}/{s3_key_dry_run}")
            # Cleanup the temporary script file
            if os.path.exists(script_path):
                os.remove(script_path)
            return {
                "status": "dry_run_success",
                "message": "Script validated (not executed)",
                "script_path": script_path
            }
        
        # 3. Generate presigned S3 upload URL (no credentials exposed to script)
        target = manifest.get("target", {})
        bucket = target.get("bucket", config.bucket_name)
        layer = target.get("layer", "landing")
        source_name = target.get("source", "unknown")
        dataset = target.get("dataset", "data")
        
        # Create S3 credential service
        s3_service = S3CredentialService(
            endpoint_url=config.ovh_endpoint,
            region_name=config.ovh_region,
            access_key=config.ovh_access_key,
            secret_key=config.ovh_secret_key,
            default_expiration=config.presigned_url_expiration
        )
        
        # Generate presigned upload URL
        s3_key = f"{layer}/{source_name}/{dataset}/data.json"
        presigned_upload_url = s3_service.generate_presigned_upload_url(
            bucket=bucket,
            key=s3_key,
            expiration=config.script_execution_timeout + 300  # Timeout + 5min buffer
        )
        
        logger.info(f"[{self.name}] Generated presigned upload URL for s3://{bucket}/{s3_key}")
        
        # 4. Execute the script with presigned URL (NO CREDENTIALS)
        env_vars = os.environ.copy()
        env_vars.update({
            "PYTHONPATH": os.getcwd(),
            "S3_UPLOAD_URL": presigned_upload_url,  # ✅ Presigned URL, not credentials
            "S3_BUCKET": bucket,
            "S3_KEY": s3_key,
        })

        try:
            # Use subprocess timeout for cross-platform compatibility
            # time_limit provides additional safety on Unix/Linux
            with time_limit(config.script_execution_timeout):
                result = subprocess.run(
                    [sys.executable, script_path],
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=config.script_execution_timeout,  # Cross-platform timeout
                    env=env_vars
                )
            logger.info(f"[{self.name}] Execution successful:\n{result.stdout}")
            return {"status": "success", "output": result.stdout}
            
        except subprocess.TimeoutExpired as e:
            logger.error(f"[{self.name}] Script execution timed out after {config.script_execution_timeout}s")
            return {"status": "failed", "error": "Script execution timed out"}
            
        except TimeoutException as e:
            # Unix/Linux signal-based timeout
            logger.error(f"[{self.name}] {str(e)}")
            return {"status": "failed", "error": "Script execution timed out"}
            
        except subprocess.CalledProcessError as e:
            logger.error(f"[{self.name}] Execution failed:\n{e.stderr}")
            # Optional: Implement feedback loop here to fix the script
            return {"status": "failed", "error": e.stderr}
        finally:
            # Cleanup
            if os.path.exists(script_path):
                os.remove(script_path)

    def _generate_and_validate_script(self, manifest: Dict[str, Any]) -> Optional[str]:
        """Generate script with validation and retry logic."""
        for attempt in range(self.max_retries):
            logger.info(f"[{self.name}] Script generation attempt {attempt + 1}/{self.max_retries}")
            
            # Generate script
            script_content = self._generate_script(manifest)
            if not script_content:
                logger.warning(f"[{self.name}] Failed to extract code from LLM response")
                continue
            
            # Validate script
            is_valid, error_msg, suggestions = self.validator.validate(script_content)
            
            if is_valid:
                logger.info(f"[{self.name}] ✅ Script validation passed")
                return script_content
            
            # Validation failed - provide feedback to LLM
            logger.warning(f"[{self.name}] ❌ Script validation failed: {error_msg}")
            
            if attempt < self.max_retries - 1:
                # Retry with feedback
                feedback = (
                    f"The previous script had validation errors:\n"
                    f"ERROR: {error_msg}\n\n"
                    f"SUGGESTIONS:\n" + "\n".join(f"- {s}" for s in suggestions) + "\n\n"
                    f"Please generate a corrected version that fixes these issues."
                )
                logger.info(f"[{self.name}] Retrying with feedback to LLM")
                # Add feedback to history for next attempt
                self.history.append({"role": "user", "content": feedback})
            else:
                logger.error(f"[{self.name}] Max retries reached. Validation report:\n{self.validator.get_validation_report()}")
        
        return None
    
    def _generate_script(self, manifest: Dict[str, Any]) -> Optional[str]:
        """Prompts the LLM to generate the Python ingestion script."""
        source = manifest.get("source", {})
        target = manifest.get("target", {})
        
        prompt = (
            f"Write a standalone Python script to ingest data based on this configuration:\n\n"
            f"SOURCE:\n"
            f"- URL: {source.get('url')}\n"
            f"- Method: {source.get('method', 'GET')}\n"
            f"- Format: {source.get('format', 'json')}\n"
            f"- Pagination: {source.get('pagination', {})}\n\n"
            f"TARGET S3 CONFIG:\n"
            f"- Bucket: {target.get('bucket', config.bucket_name)}\n"
            f"- Base Path: layer={target.get('layer', 'landing')}/source={target.get('source', 'unknown')}/dataset={target.get('dataset', 'data')}\n\n"
            "REQUIREMENTS:\n"
            "1. Use `requests` to fetch data. Handle pagination automatically.\n"
            "2. Upload data to S3 using PRESIGNED URL (NO boto3 or credentials needed):\n"
            "   - Get presigned upload URL from os.environ['S3_UPLOAD_URL']\n"
            "   - Use requests.put(url, data=json_data, headers={'Content-Type': 'application/json'})\n"
            "   - DO NOT use boto3, AWS credentials, or any S3 client libraries\n"
            "   - The presigned URL handles all authentication\n"
            f"3. Upload the data to S3 using the presigned URL (PUT request, JSON body).\n"
            f"4. Handle errors gracefully (retry logic for API calls, skip bad records).\n"
            f"5. Print summary logs (records fetched, errors encountered).\n"
            f"6. SECURITY: Only use safe imports. DO NOT use os.system, subprocess.Popen, eval, exec, or __import__.\n"
            f"\n"
            f"TEMPLATE REFERENCE (follow this pattern):\n"
            f"```python\n"
            f"# For paginated APIs, use this pattern:\n"
            f"import requests\n"
            f"import json\n"
            f"import os\n"
            f"\n"
            f"API_URL = os.environ['API_URL']\n"
            f"S3_UPLOAD_URL = os.environ['S3_UPLOAD_URL']\n"
            f"\n"
            f"all_data = []\n"
            f"page = 1\n"
            f"\n"
            f"while True:\n"
            f"    response = requests.get(f'{{API_URL}}?page={{page}}', timeout=30)\n"
            f"    response.raise_for_status()\n"
            f"    data = response.json()\n"
            f"    \n"
            f"    if not data:\n"
            f"        break\n"
            f"    \n"
            f"    all_data.extend(data)\n"
            f"    page += 1\n"
            f"\n"
            f"# Upload to S3\n"
            f"requests.put(S3_UPLOAD_URL, data=json.dumps(all_data), headers={{'Content-Type': 'application/json'}})\n"
            f"```\n"
        )
        
        response = self.chat(prompt)
        
        return self._extract_code_from_response(response)
        
    def _extract_code_from_response(self, response: str) -> Optional[str]:
        """Extract Python code from LLM response with validation."""
        # Try to find code block with ```python
        pattern = r"```python\s*\n(.*?)\n```"
        match = re.search(pattern, response, re.DOTALL)
        
        if match:
            code = match.group(1).strip()
            if self._validate_syntax(code):
                return code
            logger.warning("Code block found but has syntax errors")
        
        # Fallback: try generic code block
        pattern = r"```\s*\n(.*?)\n```"
        match = re.search(pattern, response, re.DOTALL)
        
        if match:
            code = match.group(1).strip()
            if self._validate_syntax(code):
                return code
            logger.warning("Generic code block found but has syntax errors")
        
        # No valid code block found
        logger.error("No valid Python code block found in LLM response")
        return None
    
    def _validate_syntax(self, code: str) -> bool:
        """Validate Python syntax using AST parsing."""
        try:
            ast.parse(code)
            logger.debug("Code syntax validation: PASSED")
            return True
        except SyntaxError as e:
            logger.error(f"Syntax validation failed: {e.msg} at line {e.lineno}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during syntax validation: {e}")
            return False
