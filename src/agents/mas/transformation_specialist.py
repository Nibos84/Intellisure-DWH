import logging
import json
import subprocess
import os
import sys
import re
from typing import Dict, Any, List, Optional
from src.agents.mas.base_role import AgentRole
from src.core.s3_manager import s3_manager
from src.core.config import config
from src.security.code_validator import CodeValidator
from src.utils.execution import time_limit, TimeoutException

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
        
        # 1. Get sample data for context
        source_path = source_config.get("path", "")
        sample_data = self._get_sample_data(source_path)
        
        if not sample_data:
            logger.warning(f"[{self.name}] No data found in {source_path}")
            return {"status": "no_data"}

        # 2. Generate and validate the transformation script (with retries)
        script_content = self._generate_and_validate_script(manifest, sample_data)
        if not script_content:
            return {"status": "failed", "error": "Failed to generate valid script after retries"}

        # 3. Save script to file
        script_path = f"transform_{pipeline_name}.py"
        with open(script_path, "w") as f:
            f.write(script_content)

        logger.info(f"[{self.name}] Generated script saved to {script_path}")
        
        # 4. Execute the script
        env_vars = os.environ.copy()
        env_vars.update({
            "PYTHONPATH": os.getcwd(),
            "OVH_ENDPOINT": config.ovh_endpoint,
            "OVH_REGION": config.ovh_region,
            "OVH_ACCESS_KEY": config.ovh_access_key,
            "OVH_SECRET_KEY": config.ovh_secret_key,
        })
        
        try:
            # Pass sample_data or file list if needed, but script should handle listing
            with time_limit(config.script_execution_timeout):
                result = subprocess.run(
                    [sys.executable, script_path],
                    capture_output=True,
                    text=True,
                    check=True,
                    env=env_vars
                )
            logger.info(f"[{self.name}] Execution successful:\n{result.stdout}")
            return {"status": "success", "output": result.stdout}

        except TimeoutException as e:
            logger.error(f"[{self.name}] {str(e)}")
            return {"status": "failed", "error": "Script execution timed out"}

        except subprocess.CalledProcessError as e:
            logger.error(f"[{self.name}] Execution failed:\n{e.stderr}")
            # Optional: Implement feedback loop here
            return {"status": "failed", "error": e.stderr}
        finally:
            if os.path.exists(script_path):
                os.remove(script_path)
    
    def _get_sample_data(self, source_path: str) -> Optional[str]:
        """Reads a sample of the first file in the source path."""
        files = s3_manager.list_files(source_path)
        if not files:
            return None

        first_file = files[0]
        content = s3_manager.read_file(first_file)
        if not content:
            return None

        try:
            text = content.decode('utf-8')
            return text[:5000] # Return first 5KB
        except UnicodeDecodeError:
            return str(content)[:5000]

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
            f"- Bucket: {target.get('bucket', config.bucket_name)}\n"
            f"S3 CONFIG:\n"
            f"- Endpoint: Read from env OVH_ENDPOINT\n"
            f"- Region: Read from env OVH_REGION\n"
            f"- Access Key: Read from env OVH_ACCESS_KEY\n"
            f"- Secret Key: Read from env OVH_SECRET_KEY\n\n"
            f"TRANSFORMATION INSTRUCTION: {ai_config.get('instruction')}\n"
            f"TARGET SCHEMA: {json.dumps(ai_config.get('schema'), indent=2)}\n\n"
            f"SAMPLE SOURCE DATA (First 5KB):\n{sample_data}\n\n"
            "REQUIREMENTS:\n"
            "1. Use `boto3` to list all files in the source prefix.\n"
            "2. For each file:\n"
            "   a. Download it locally (or read into memory).\n"
            "   b. Load into a Pandas DataFrame (infer format from extension or content).\n"
            "   c. Apply transformations to match the schema and instruction.\n"
            "   d. Convert data types if necessary (e.g., date strings to datetime objects).\n"
            "   e. Write the result to the target path (change extension to .parquet or .json).\n"
            "   f. Use `boto3` to upload the result.\n"
            "   - Initialize boto3 client using `os.environ` variables: OVH_ENDPOINT, OVH_REGION, OVH_ACCESS_KEY, OVH_SECRET_KEY.\n"
            "3. Handle errors gracefully (skip bad files and log errors).\n"
            "4. Print summary logs (processed count, error count).\n"
            "5. The script must be self-contained.\n"
            "6. SECURITY: Only use safe imports. Do NOT use os.system, subprocess.Popen, eval, exec, or __import__.\n"
        )
        
        response = self.chat(prompt)
        
        match = re.search(r"```python\n(.*?)\n```", response, re.DOTALL)
        if match:
            return match.group(1)

        if "import pandas" in response:
             return response.replace("```python", "").replace("```", "")

        return None
