"""
Unit tests for CodeValidator security module.
"""

import pytest
from src.security.code_validator import CodeValidator


class TestCodeValidator:
    """Test suite for code validation and security checks."""
    
    @pytest.fixture
    def validator(self):
        """Create a fresh validator instance for each test."""
        return CodeValidator()
    
    # ===== DANGEROUS IMPORTS TESTS =====
    
    def test_block_os_system(self, validator):
        """Should block os.system imports."""
        dangerous_code = '''
import os
os.system("rm -rf /")
'''
        is_valid, error, _ = validator.validate(dangerous_code)
        assert not is_valid
        assert "dangerous import" in error.lower() or "os" in error.lower()
    
    def test_block_subprocess(self, validator):
        """Should block subprocess imports."""
        dangerous_code = '''
import subprocess
subprocess.run(["rm", "-rf", "/"])
'''
        is_valid, error, _ = validator.validate(dangerous_code)
        assert not is_valid
        assert "subprocess" in error.lower()
    
    def test_block_eval(self, validator):
        """Should block eval imports."""
        dangerous_code = '''
user_input = "print('hacked')"
eval(user_input)
'''
        is_valid, error, _ = validator.validate(dangerous_code)
        assert not is_valid
        assert "eval" in error.lower()
    
    def test_block_exec(self, validator):
        """Should block exec calls."""
        dangerous_code = '''
code = "import os; os.system('echo hacked')"
exec(code)
'''
        is_valid, error, _ = validator.validate(dangerous_code)
        assert not is_valid
        assert "exec" in error.lower()
    
    def test_block_import_builtin(self, validator):
        """Should block __import__ usage."""
        dangerous_code = '''
os = __import__('os')
os.system('echo hacked')
'''
        is_valid, error, _ = validator.validate(dangerous_code)
        assert not is_valid
    
    def test_block_socket(self, validator):
        """Should block socket imports."""
        dangerous_code = '''
import socket
s = socket.socket()
s.connect(("evil.com", 1234))
'''
        is_valid, error, _ = validator.validate(dangerous_code)
        assert not is_valid
        assert "socket" in error.lower()
    
    def test_block_pickle(self, validator):
        """Should block pickle imports (arbitrary code execution risk)."""
        dangerous_code = '''
import pickle
data = pickle.loads(untrusted_data)
'''
        is_valid, error, _ = validator.validate(dangerous_code)
        assert not is_valid
        assert "pickle" in error.lower()
    
    # ===== SAFE IMPORTS TESTS =====
    
    def test_allow_pandas(self, validator):
        """Should allow pandas imports."""
        safe_code = '''
import pandas as pd
df = pd.DataFrame({'a': [1, 2, 3]})
print(df.head())
'''
        is_valid, error, _ = validator.validate(safe_code)
        assert is_valid
        assert error is None
    
    def test_allow_boto3(self, validator):
        """Should allow boto3 imports."""
        safe_code = '''
import boto3
s3 = boto3.client('s3')
s3.list_buckets()
'''
        is_valid, error, _ = validator.validate(safe_code)
        assert is_valid
        assert error is None
    
    def test_allow_requests(self, validator):
        """Should allow requests imports."""
        safe_code = '''
import requests
response = requests.get("https://api.example.com/data")
print(response.json())
'''
        is_valid, error, _ = validator.validate(safe_code)
        assert is_valid
        assert error is None
    
    def test_allow_json(self, validator):
        """Should allow json imports."""
        safe_code = '''
import json
data = json.loads('{"key": "value"}')
print(data)
'''
        is_valid, error, _ = validator.validate(safe_code)
        assert is_valid
        assert error is None
    
    def test_allow_datetime(self, validator):
        """Should allow datetime imports."""
        safe_code = '''
from datetime import datetime, timedelta
now = datetime.now()
tomorrow = now + timedelta(days=1)
'''
        is_valid, error, _ = validator.validate(safe_code)
        assert is_valid
        assert error is None
    
    def test_allow_multiple_safe_imports(self, validator):
        """Should allow multiple safe imports together."""
        safe_code = '''
import pandas as pd
import boto3
import requests
import json
from datetime import datetime

df = pd.DataFrame({'date': [datetime.now()]})
'''
        is_valid, error, _ = validator.validate(safe_code)
        assert is_valid
        assert error is None
    
    # ===== SYNTAX VALIDATION TESTS =====
    
    def test_detect_syntax_error(self, validator):
        """Should detect syntax errors."""
        invalid_syntax = '''
def foo(
    print("missing closing paren")
'''
        is_valid, error, _ = validator.validate(invalid_syntax)
        assert not is_valid
        assert "syntax" in error.lower()
    
    def test_detect_indentation_error(self, validator):
        """Should detect indentation errors."""
        invalid_syntax = '''
def foo():
print("bad indentation")
'''
        is_valid, error, _ = validator.validate(invalid_syntax)
        assert not is_valid
    
    def test_detect_unclosed_string(self, validator):
        """Should detect unclosed strings."""
        invalid_syntax = '''
message = "unclosed string
print(message)
'''
        is_valid, error, _ = validator.validate(invalid_syntax)
        assert not is_valid
    
    # ===== COMPILATION TESTS =====
    
    def test_valid_code_compiles(self, validator):
        """Valid code should compile successfully."""
        valid_code = '''
def process_data(data):
    result = []
    for item in data:
        result.append(item * 2)
    return result

data = [1, 2, 3]
output = process_data(data)
print(output)
'''
        is_valid, error, _ = validator.validate(valid_code)
        assert is_valid
        assert error is None
    
    # ===== COMPLEX SCENARIOS =====
    
    def test_realistic_ingestion_script(self, validator):
        """Should validate a realistic ingestion script."""
        ingestion_script = '''
import requests
import boto3
import json
from datetime import datetime
import os

# Configuration
API_URL = "https://api.example.com/data"
BUCKET = "my-bucket"
PREFIX = "layer=landing/source=api/dataset=data"

# Initialize S3 client
s3_client = boto3.client(
    's3',
    endpoint_url=os.environ.get('OVH_ENDPOINT'),
    aws_access_key_id=os.environ.get('OVH_ACCESS_KEY'),
    aws_secret_access_key=os.environ.get('OVH_SECRET_KEY'),
    region_name=os.environ.get('OVH_REGION')
)

# Fetch data
response = requests.get(API_URL)
data = response.json()

# Upload to S3
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
key = f"{PREFIX}/year={datetime.now().year}/month={datetime.now().month:02d}/batch_{timestamp}.json"

s3_client.put_object(
    Bucket=BUCKET,
    Key=key,
    Body=json.dumps(data)
)

print(f"Uploaded {len(data)} records to {key}")
'''
        is_valid, error, _ = validator.validate(ingestion_script)
        assert is_valid
        assert error is None
    
    def test_realistic_transformation_script(self, validator):
        """Should validate a realistic transformation script."""
        transformation_script = '''
import pandas as pd
import boto3
import json
from datetime import datetime
import os

# Initialize S3 client
s3_client = boto3.client(
    's3',
    endpoint_url=os.environ.get('OVH_ENDPOINT'),
    aws_access_key_id=os.environ.get('OVH_ACCESS_KEY'),
    aws_secret_access_key=os.environ.get('OVH_SECRET_KEY'),
    region_name=os.environ.get('OVH_REGION')
)

# List source files
response = s3_client.list_objects_v2(
    Bucket='my-bucket',
    Prefix='layer=landing/source=api'
)

processed = 0
for obj in response.get('Contents', []):
    # Read file
    file_obj = s3_client.get_object(Bucket='my-bucket', Key=obj['Key'])
    data = json.loads(file_obj['Body'].read())
    
    # Transform with pandas
    df = pd.DataFrame(data)
    df['processed_date'] = datetime.now()
    df['amount'] = df['amount'].astype(float)
    
    # Write to target
    output_key = obj['Key'].replace('landing', 'silver')
    s3_client.put_object(
        Bucket='my-bucket',
        Key=output_key,
        Body=df.to_parquet()
    )
    processed += 1

print(f"Processed {processed} files")
'''
        is_valid, error, _ = validator.validate(transformation_script)
        assert is_valid
        assert error is None
    
    def test_mixed_dangerous_and_safe(self, validator):
        """Should block code with both safe and dangerous imports."""
        mixed_code = '''
import pandas as pd
import boto3
import subprocess  # Dangerous!

df = pd.DataFrame({'a': [1, 2, 3]})
subprocess.run(["ls", "-la"])  # Should be blocked
'''
        is_valid, error, _ = validator.validate(mixed_code)
        assert not is_valid
        assert "subprocess" in error.lower()
    
    # ===== SUGGESTIONS TESTS =====
    
    def test_provides_suggestions(self, validator):
        """Should provide helpful suggestions for errors."""
        dangerous_code = '''
import os
os.system("echo test")
'''
        is_valid, error, suggestions = validator.validate(dangerous_code)
        assert not is_valid
        assert len(suggestions) > 0
        assert any("remove" in s.lower() or "not allowed" in s.lower() for s in suggestions)
    
    def test_validation_report(self, validator):
        """Should generate a detailed validation report."""
        dangerous_code = '''
import subprocess
subprocess.run(["echo", "test"])
'''
        validator.validate(dangerous_code)
        report = validator.get_validation_report()
        
        assert "ERROR" in report or "error" in report.lower()
        assert "subprocess" in report.lower()
    
    # ===== EDGE CASES =====
    
    def test_empty_code(self, validator):
        """Should handle empty code."""
        is_valid, error, _ = validator.validate("")
        # Empty code is technically valid Python (does nothing)
        assert is_valid
    
    def test_comments_only(self, validator):
        """Should handle code with only comments."""
        comments_only = '''
# This is a comment
# Another comment
'''
        is_valid, error, _ = validator.validate(comments_only)
        assert is_valid
    
    def test_whitespace_only(self, validator):
        """Should handle whitespace-only code."""
        whitespace = "   \n\n   \t\t\n   "
        is_valid, error, _ = validator.validate(whitespace)
        assert is_valid


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
