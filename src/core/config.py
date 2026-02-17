import os
import logging
from enum import Enum, auto
from pydantic import BaseModel
from dotenv import load_dotenv

# Load environment variables (default to .env)
load_dotenv()

class Environment(str, Enum):
    LOCAL = "local"
    DEV = "dev"
    PRD = "prd"

class AppConfig(BaseModel):
    env: Environment
    log_level: str = "INFO"
    bucket_name: str
    ovh_endpoint: str
    ovh_region: str
    ovh_access_key: str
    ovh_secret_key: str
    llm_model: str = "gpt-3.5-turbo"
    script_execution_timeout: int = 300
    presigned_url_expiration: int = 3600  # 1 hour default
    sample_data_size: int = 5000  # Sample size for data extraction in transformation scripts
    dry_run: bool = False  # Dry-run mode (validate scripts without executing)
    structured_logging: bool = False  # Use JSON structured logging
    
    @property
    def is_dev(self) -> bool:
        return self.env in (Environment.LOCAL, Environment.DEV)

    @property
    def is_prd(self) -> bool:
        return self.env == Environment.PRD

def get_config() -> AppConfig:
    def get_bucket_name(env: Environment) -> str:
        if env == Environment.PRD:
            return os.getenv("DATA_LAKE_BUCKET_PRD", os.getenv("DATA_LAKE_BUCKET", "default-bucket"))
        # Default to DEV bucket for local/dev
        return os.getenv("DATA_LAKE_BUCKET_DEV", os.getenv("DATA_LAKE_BUCKET", "default-bucket-dev"))

    env_str = os.getenv("ENV", "local").lower()
    
    try:
        env_enum = Environment(env_str)
    except ValueError:
        logging.warning(f"Invalid ENV '{env_str}', defaulting to LOCAL")
        env_enum = Environment.LOCAL
        
    return AppConfig(
        env=env_enum,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        bucket_name=get_bucket_name(env_enum),
        ovh_endpoint=os.getenv("OVH_ENDPOINT_URL", ""),
        ovh_access_key=os.getenv("OVH_ACCESS_KEY", ""),
        ovh_secret_key=os.getenv("OVH_SECRET_KEY", ""),
        ovh_region=os.getenv("OVH_REGION_NAME", "rbx"),
        llm_model=os.getenv("LLM_MODEL", "gpt-3.5-turbo"),
        script_execution_timeout=int(os.getenv("SCRIPT_EXECUTION_TIMEOUT", "300")),
        presigned_url_expiration=int(os.getenv("PRESIGNED_URL_EXPIRATION", "3600")),
        sample_data_size=int(os.getenv("SAMPLE_DATA_SIZE", "5000")),
        dry_run=os.getenv("DRY_RUN", "false").lower() == "true",
        structured_logging=os.getenv("STRUCTURED_LOGGING", "false").lower() == "true",
    )

# Singleton instance
config = get_config()

# Configure Logging
logging.basicConfig(
    level=getattr(logging, config.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)
