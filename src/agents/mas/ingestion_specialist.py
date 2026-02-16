import logging
import subprocess
import os
import sys
import re
from typing import Dict, Any, Optional
from src.agents.mas.base_role import AgentRole
from src.core.config import config
from src.security.code_validator import CodeValidator
from src.utils.execution import time_limit, TimeoutException

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
        logger.info(f"[{self.name}] Starting ingestion for: {pipeline_name}")
        
        # 1. Generate and validate the ingestion script (with retries)
        script_content = self._generate_and_validate_script(manifest)
        if not script_content:
            return {"status": "failed", "error": "Failed to generate valid script after retries"}
            
        # 2. Save script to file
        script_path = f"ingest_{pipeline_name}.py"
        with open(script_path, "w") as f:
            f.write(script_content)

        logger.info(f"[{self.name}] Generated script saved to {script_path}")
        
        # 3. Execute the script with environment variables for secrets
        env_vars = os.environ.copy()
        env_vars.update({
            "PYTHONPATH": os.getcwd(),
            "OVH_ENDPOINT": config.ovh_endpoint,
            "OVH_REGION": config.ovh_region,
            "OVH_ACCESS_KEY": config.ovh_access_key,
            "OVH_SECRET_KEY": config.ovh_secret_key,
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
            "2. Use `boto3` to upload data to S3. \n"
            "   - Initialize boto3 client using `os.environ` variables: OVH_ENDPOINT, OVH_REGION, OVH_ACCESS_KEY, OVH_SECRET_KEY.\n"
            "   - Create partitioned path: {Base Path}/year=YYYY/month=MM/day=DD/batch_{timestamp}.json\n"
            "3. Implement retry logic for network requests.\n"
            "4. Print JSON logs to stdout for monitoring.\n"
            "5. The script must be self-contained (import os, sys, requests, boto3, etc).\n"
            "6. Handle errors and exit with non-zero status code on failure.\n"
            "7. SECURITY: Only use safe imports. Do NOT use os.system, subprocess.Popen, eval, exec, or __import__.\n"
        )
        
        response = self.chat(prompt)
        
        # Extract code block
        match = re.search(r"```python\n(.*?)\n```", response, re.DOTALL)
        if match:
            return match.group(1)
        
        # Fallback if no block found but code looks present
        if "import requests" in response:
             return response.replace("```python", "").replace("```", "")

        return None
