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

    @property
    def is_dev(self) -> bool:
        return self.env in (Environment.LOCAL, Environment.DEV)

    @property
    def is_prd(self) -> bool:
        return self.env == Environment.PRD

def get_config() -> AppConfig:
    env_str = os.getenv("ENV", "local").lower()
    
    # Validation of env specific variables could be added here
    try:
        env = Environment(env_str)
    except ValueError:
        logging.warning(f"Invalid ENV '{env_str}', defaulting to LOCAL")
        env = Environment.LOCAL
        
    return AppConfig(
        env=env,
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        bucket_name=os.getenv("DATA_LAKE_BUCKET", "default-bucket"),
        ovh_endpoint=os.getenv("OVH_ENDPOINT_URL", ""),
        ovh_access_key=os.getenv("OVH_ACCESS_KEY", ""),
        ovh_secret_key=os.getenv("OVH_SECRET_KEY", ""),
        ovh_region=os.getenv("OVH_REGION_NAME", "rbx"),
    )

# Singleton instance
config = get_config()

# Configure Logging
logging.basicConfig(
    level=getattr(logging, config.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
