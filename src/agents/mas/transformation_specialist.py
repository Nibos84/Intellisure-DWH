import logging
import json
import subprocess
import os
import sys
import re
import ast
from typing import Dict, Any, List, Optional
from src.agents.mas.base_role import AgentRole
from src.core.config import config
from src.storage.s3_manager import S3Manager
from src.security.code_validator import CodeValidator
from src.security.s3_credential_service import S3CredentialService
from src.utils.execution import time_limit, TimeoutException
from src.utils.script_cache import get_script_cache

logger = logging.getLogger(__name__)

class TransformationSpecialistAgent(AgentRole):
    """
    Code-generating transformation agent.
    Generates and executes Python scripts (using Pandas) to transform data.
    """
    def __init__(self):
        super().__init__(
            name="Transformation Specialist",
            role="Data Transformation Expert",
            goal="Generate and execute efficient Python scripts using Pandas to transform raw data into clean, structured formats."
        )
        self.validator = CodeValidator()
        self.max_retries = 3
        self.system_prompt = (
            "You are an expert Data Engineer specializing in data transformation using Python and Pandas.\n"
            "Your goal is to write efficient scripts that read data from S3, apply transformations, and write back to S3.\n"
            "The scripts must handle data quality issues, enforce schemas, and work with large datasets.\n"
            "You must output ONLY the Python code within a ```python block.\n"
            "Do NOT provide explanations or commentary outside the code block.\n"
            "IMPORTANT: Only use safe imports (pandas, boto3, numpy, json, datetime). "
            "Do NOT use os.system, subprocess, eval, exec, or other dangerous functions."
        )
        
    def execute(self, manifest: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the transformation pipeline by generating and running a script.
        """
        pipeline_name = manifest.get("pipeline_name", "unknown")
        source_config = manifest.get("source", {})
        target_config = manifest.get("target", {})
        ai_config = manifest.get("ai_config", {})
        
        logger.info(f"[{self.name}] Starting transformation: {pipeline_name}")
        
        # 1. Get sample data from source
        sample_data = self._get_sample_data(manifest)
        if not sample_data:
            return {"status": "failed", "error": "Failed to retrieve sample data"}
        
        # 2. Check cache first
        cache = get_script_cache()
        cached_script = cache.get(manifest)
        
        if cached_script:
            logger.info(f"[{self.name}] Using cached script for {pipeline_name}")
            script_content = cached_script
        else:
            # 3. Generate and validate transformation script
            script_content = self._generate_and_validate_script(manifest, sample_data)
            if not script_content:
                return {"status": "failed", "error": "Failed to generate valid script after retries"}
            
            # Store in cache for future use
            cache.set(manifest, script_content)
            logger.info(f"[{self.name}] Script generated and cached for {pipeline_name}")

        # 4. Save script to file with unique name to prevent race conditions
        import uuid
        script_path = f"transform_{pipeline_name}_{uuid.uuid4().hex[:8]}.py"
        with open(script_path, "w") as f:
            f.write(script_content)

        logger.info(f"[{self.name}] Generated script saved to {script_path}")
        
        # Check dry-run mode
        if config.dry_run:
            source_config = manifest.get("source", {})
            target_config = manifest.get("target", {})
            logger.info(f"[DRY-RUN] Would execute script: {script_path}")
            logger.info(f"[DRY-RUN] Script validated successfully (AST + CodeValidator)")
            logger.info(f"[DRY-RUN] Source: {source_config.get('bucket')}/{source_config.get('path')}")
            logger.info(f"[DRY-RUN] Target: {target_config.get('bucket')}/{target_config.get('path')}")
            return {
                "status": "dry_run_success",
                "message": "Script validated (not executed)",
                "script_path": script_path
            }
        
        # 4. Generate presigned S3 URLs (no credentials exposed to script)
        source = manifest.get("source", {})
        target = manifest.get("target", {})
        
        source_bucket = source.get("bucket", config.bucket_name)
        source_path = source.get("path", "")
        target_bucket = target.get("bucket", config.bucket_name)
        target_path = target.get("path", "")
        
        # Create S3 credential service
        s3_service = S3CredentialService(
            endpoint_url=config.ovh_endpoint,
            region_name=config.ovh_region,
            access_key=config.ovh_access_key,
            secret_key=config.ovh_secret_key,
            default_expiration=config.presigned_url_expiration
        )
        
        # Generate presigned URLs for download and upload
        presigned_download_url = s3_service.generate_presigned_download_url(
            bucket=source_bucket,
            key=source_path,
            expiration=config.script_execution_timeout + 300  # Timeout + 5min buffer
        )
        
        presigned_upload_url = s3_service.generate_presigned_upload_url(
            bucket=target_bucket,
            key=target_path,
            expiration=config.script_execution_timeout + 300
        )
        
        logger.info(
            f"[{self.name}] Generated presigned URLs:\n"
            f"  Download: s3://{source_bucket}/{source_path}\n"
            f"  Upload: s3://{target_bucket}/{target_path}"
        )
        
        # 5. Execute the script with presigned URLs (NO CREDENTIALS)
        env_vars = os.environ.copy()
        env_vars.update({
            "PYTHONPATH": os.getcwd(),
            "S3_DOWNLOAD_URL": presigned_download_url,  # ✅ Presigned URL, not credentials
            "S3_UPLOAD_URL": presigned_upload_url,      # ✅ Presigned URL, not credentials
            "SOURCE_BUCKET": source_bucket,
            "SOURCE_PATH": source_path,
            "TARGET_BUCKET": target_bucket,
            "TARGET_PATH": target_path,
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
            # Optional: Implement feedback loop here
            return {"status": "failed", "error": e.stderr}
        finally:
            if os.path.exists(script_path):
                os.remove(script_path)
                logger.info(f"[{self.name}] Cleaned up temporary script: {script_path}")
    
    def _get_sample_data(self, manifest: Dict[str, Any]) -> Optional[str]:
        """Get sample data from source for LLM context."""
        source_config = manifest.get("source", {})
        source_path = source_config.get("path", "")
        
        s3_manager = S3Manager()
        files = s3_manager.list_files(source_path)
        if not files:
            return None

        first_file = files[0]
        content = s3_manager.read_file(first_file)
        if not content:
            return None

        try:
            text = content.decode('utf-8')
            return text[:config.sample_data_size]  # Configurable sample size
        except UnicodeDecodeError:
            return str(content)[:config.sample_data_size]

    def _generate_and_validate_script(self, manifest: Dict[str, Any], sample_data: str) -> Optional[str]:
        """Generate script with validation and retry logic."""
        for attempt in range(self.max_retries):
            logger.info(f"[{self.name}] Script generation attempt {attempt + 1}/{self.max_retries}")
            
            # Generate script
            script_content = self._generate_script(manifest, sample_data)
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
    
    def _generate_script(self, manifest: Dict[str, Any], sample_data: str) -> Optional[str]:
        """Prompts the LLM to generate the Python transformation script."""
        source = manifest.get("source", {})
        target = manifest.get("target", {})
        ai_config = manifest.get("ai_config", {})
        
        prompt = (
            f"Write a standalone Python script to transform data based on this configuration:\n\n"
            f"SOURCE S3:\n"
            f"- Path Prefix: {source.get('path')}\n"
            f"- Bucket: {source.get('bucket', config.bucket_name)}\n"
            f"TARGET S3:\n"
            f"- Path Prefix: {target.get('path')}\n"
            f"- Bucket: {target.get('bucket', config.bucket_name)}\n\n"
            f"TRANSFORMATION INSTRUCTION: {ai_config.get('instruction')}\n"
            f"TARGET SCHEMA: {json.dumps(ai_config.get('schema'), indent=2)}\n\n"
            f"SAMPLE SOURCE DATA (First 5KB):\n{sample_data}\n\n"
            "REQUIREMENTS:\n"
            "1. Download source data using PRESIGNED URL (NO boto3 needed):\n"
            "   - Use requests.get(os.environ['S3_DOWNLOAD_URL']) to download\n"
            "   - DO NOT use boto3 or AWS credentials\n"
            "2. For each file/record:\n"
            "   a. Load into a Pandas DataFrame (infer format from extension or content).\n"
            "   b. Apply transformations to match the schema and instruction.\n"
            "   c. Convert data types if necessary (e.g., date strings to datetime objects).\n"
            "3. Upload result using PRESIGNED URL:\n"
            "   - Use requests.put(os.environ['S3_UPLOAD_URL'], data=result, headers={'Content-Type': 'application/json'})\n"
            "   - DO NOT use boto3 or AWS credentials\n"
            "4. Handle errors gracefully (skip bad files and log errors).\n"
            "5. Print summary logs (processed count, error count).\n"
            f"6. Handle errors and exit with non-zero status code on failure.\n"
            f"7. SECURITY: Only use safe imports. Do NOT use os.system, subprocess.Popen, eval, exec, or __import__.\n"
        )
        
        # Add schema validation if schema is provided
        if schema:
            prompt += (
                f"\n\n8. SCHEMA VALIDATION (CRITICAL):\n"
                f"   After transformations, validate that data types match the expected schema:\n"
                f"   Expected schema: {json.dumps(schema, indent=2)}\n\n"
                f"   Validation code template:\n"
                f"   ```python\n"
                f"   # Validate schema\n"
                f"   expected_schema = {json.dumps(schema)}\n"
                f"   actual_dtypes = df.dtypes.astype(str).to_dict()\n"
                f"   \n"
                f"   mismatches = []\n"
                f"   for col, expected_type in expected_schema.items():\n"
                f"       if col not in df.columns:\n"
                f"           mismatches.append(f'Missing column: {{col}}')\n"
                f"       else:\n"
                f"           actual = str(df[col].dtype)\n"
                f"           # Map pandas types to schema types\n"
                f"           if expected_type == 'int' and 'int' not in actual:\n"
                f"               mismatches.append(f'{{col}}: expected int, got {{actual}}')\n"
                f"           elif expected_type == 'str' and actual != 'object':\n"
                f"               mismatches.append(f'{{col}}: expected str, got {{actual}}')\n"
                f"           elif expected_type == 'float' and 'float' not in actual:\n"
                f"               mismatches.append(f'{{col}}: expected float, got {{actual}}')\n"
                f"           elif expected_type.startswith('datetime') and 'datetime' not in actual:\n"
                f"               mismatches.append(f'{{col}}: expected datetime, got {{actual}}')\n"
                f"   \n"
                f"   if mismatches:\n"
                f"       print(json.dumps({{'error': 'Schema validation failed', 'mismatches': mismatches}}))\n"
                f"       raise ValueError(f'Schema validation failed: {{mismatches}}')\n"
                f"   \n"
                f"   print(json.dumps({{'status': 'success', 'message': 'Schema validation passed'}}))\n"
                f"   ```\n"
            )
        
        # Add transformation template reference
        prompt += (
            f"\n\nTRANSFORMATION TEMPLATE (follow this pattern):\n"
            f"```python\n"
            f"import pandas as pd\n"
            f"import requests\n"
            f"import os\n"
            f"import json\n"
            f"\n"
            f"S3_DOWNLOAD_URL = os.environ['S3_DOWNLOAD_URL']\n"
            f"S3_UPLOAD_URL = os.environ['S3_UPLOAD_URL']\n"
            f"\n"
            f"# Download data\n"
            f"response = requests.get(S3_DOWNLOAD_URL, timeout=60)\n"
            f"response.raise_for_status()\n"
            f"data = response.json()\n"
            f"df = pd.DataFrame(data)\n"
            f"\n"
            f"# Transform data\n"
            f"# ... your transformation logic here ...\n"
            f"\n"
            f"# Upload to S3\n"
            f"json_data = df.to_json(orient='records', date_format='iso')\n"
            f"requests.put(S3_UPLOAD_URL, data=json_data, headers={{'Content-Type': 'application/json'}})\n"
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
        
        # Last resort: return entire response if it looks like code
        if "import" in response or "def " in response:
            code = response.strip()
            if self._validate_syntax(code):
                return code
        
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
