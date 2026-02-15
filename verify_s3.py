from src.core.s3_manager import s3_manager
from src.core.config import config

print(f"Checking bucket: {config.bucket_name} in env: {config.env}")
# Check new Hive structure: layer=landing/source=rechtspraak
files = s3_manager.list_files(prefix="layer=landing/source=rechtspraak")
print(f"Found {len(files)} files:")
for f in files[:5]:
    print(f" - {f}")
