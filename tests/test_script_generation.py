"""
Unit tests for script generation and code extraction.

Tests the _generate_script() and code extraction methods in both
ingestion and transformation specialists.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from src.agents.mas.ingestion_specialist import IngestionSpecialistAgent
from src.agents.mas.transformation_specialist import TransformationSpecialistAgent


class TestIngestionScriptGeneration:
    """Test suite for IngestionSpecialistAgent script generation."""
    
    @pytest.fixture
    def agent(self):
        """Create ingestion specialist agent with mocked LLM client."""
        with patch('src.agents.mas.base_role.get_llm_client'):
            agent = IngestionSpecialistAgent()
            return agent
    
    @pytest.fixture
    def sample_manifest(self):
        """Sample ingestion manifest."""
        return {
            "pipeline_name": "test_api_ingestion",
            "agent_type": "ingestion",
            "source": {
                "type": "api",
                "url": "https://api.example.com/data",
                "method": "GET",
                "format": "json"
            },
            "target": {
                "bucket": "test-bucket",
                "layer": "landing",
                "source": "api",
                "dataset": "test_data"
            }
        }
    
    def test_extract_code_from_response_with_python_block(self, agent):
        """Test code extraction from response with ```python block."""
        response = """
Here's the script:

```python
import requests
import os

url = os.environ['API_URL']
response = requests.get(url)
print(response.json())
```

This script fetches data from the API.
"""
        
        code = agent._extract_code_from_response(response)
        
        assert code is not None
        assert "import requests" in code
        assert "import os" in code
        assert "requests.get" in code
    
    def test_extract_code_from_response_with_generic_block(self, agent):
        """Test code extraction from response with generic ``` block."""
        response = """
```
import requests
data = requests.get('https://api.example.com').json()
```
"""
        
        code = agent._extract_code_from_response(response)
        
        assert code is not None
        assert "import requests" in code
    
    def test_extract_code_from_response_no_block(self, agent):
        """Test code extraction fails when no code block found."""
        response = "This is just text without any code blocks."
        
        code = agent._extract_code_from_response(response)
        
        assert code is None
    
    def test_validate_syntax_valid_code(self, agent):
        """Test syntax validation with valid Python code."""
        valid_code = """
import requests
import json

def fetch_data():
    response = requests.get('https://api.example.com')
    return response.json()
"""
        
        is_valid = agent._validate_syntax(valid_code)
        
        assert is_valid is True
    
    def test_validate_syntax_invalid_code(self, agent):
        """Test syntax validation with invalid Python code."""
        invalid_code = """
import requests
def fetch_data(
    # Missing closing parenthesis
    return requests.get('url')
"""
        
        is_valid = agent._validate_syntax(invalid_code)
        
        assert is_valid is False
    
    def test_validate_syntax_empty_code(self, agent):
        """Test syntax validation with empty code."""
        is_valid = agent._validate_syntax("")
        
        # Empty code is technically valid Python
        assert is_valid is True
    
    @patch.object(IngestionSpecialistAgent, 'chat')
    def test_generate_script_success(self, mock_chat, agent, sample_manifest):
        """Test successful script generation."""
        mock_chat.return_value = """
```python
import requests
import os
import json

# Fetch data from API
api_url = os.environ['API_URL']
response = requests.get(api_url)
data = response.json()

# Upload to S3 using presigned URL
upload_url = os.environ['S3_UPLOAD_URL']
requests.put(upload_url, data=json.dumps(data), headers={'Content-Type': 'application/json'})
```
"""
        
        script = agent._generate_script(sample_manifest)
        
        assert script is not None
        assert "import requests" in script
        assert "S3_UPLOAD_URL" in script
        mock_chat.assert_called_once()
    
    @patch.object(IngestionSpecialistAgent, 'chat')
    def test_generate_script_with_syntax_error(self, mock_chat, agent, sample_manifest):
        """Test script generation with syntax error in LLM response."""
        # LLM returns code with syntax error
        mock_chat.return_value = """
```python
import requests
def fetch_data(  # Missing closing paren
    return requests.get('url')
```
"""
        
        script = agent._generate_script(sample_manifest)
        
        # Should return None due to syntax error
        assert script is None


class TestTransformationScriptGeneration:
    """Test suite for TransformationSpecialistAgent script generation."""
    
    @pytest.fixture
    def agent(self):
        """Create transformation specialist agent with mocked LLM client."""
        with patch('src.agents.mas.base_role.get_llm_client'):
            agent = TransformationSpecialistAgent()
            return agent
    
    @pytest.fixture
    def sample_manifest(self):
        """Sample transformation manifest."""
        return {
            "pipeline_name": "test_transformation",
            "agent_type": "transformation",
            "source": {
                "bucket": "source-bucket",
                "path": "landing/api/data"
            },
            "target": {
                "bucket": "target-bucket",
                "path": "staging/transformed"
            },
            "ai_config": {
                "instruction": "Transform JSON to Parquet",
                "schema": {
                    "id": "int",
                    "name": "str",
                    "created_at": "datetime64[ns]"
                }
            }
        }
    
    @pytest.fixture
    def sample_data(self):
        """Sample source data."""
        return """
{"id": 1, "name": "Alice", "created_at": "2024-01-01T10:00:00"}
{"id": 2, "name": "Bob", "created_at": "2024-01-02T11:00:00"}
"""
    
    def test_extract_code_with_last_resort(self, agent):
        """Test code extraction with last resort (no code blocks)."""
        response = """
import pandas as pd
import requests

# Download data
data = requests.get(url).json()
df = pd.DataFrame(data)
"""
        
        code = agent._extract_code_from_response(response)
        
        assert code is not None
        assert "import pandas" in code
    
    def test_validate_syntax_with_pandas_code(self, agent):
        """Test syntax validation with Pandas code."""
        pandas_code = """
import pandas as pd

df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
df['c'] = df['a'] + df['b']
"""
        
        is_valid = agent._validate_syntax(pandas_code)
        
        assert is_valid is True
    
    @patch.object(TransformationSpecialistAgent, 'chat')
    def test_generate_script_with_schema(self, mock_chat, agent, sample_manifest, sample_data):
        """Test script generation includes schema conversion."""
        mock_chat.return_value = """
```python
import pandas as pd
import requests
import os

# Download source data
download_url = os.environ['S3_DOWNLOAD_URL']
response = requests.get(download_url)
data = response.json()

# Transform
df = pd.DataFrame(data)
df['id'] = df['id'].astype('int')
df['name'] = df['name'].astype('str')
df['created_at'] = pd.to_datetime(df['created_at'])

# Upload
upload_url = os.environ['S3_UPLOAD_URL']
requests.put(upload_url, data=df.to_parquet())
```
"""
        
        script = agent._generate_script(sample_manifest, sample_data)
        
        assert script is not None
        assert "pd.DataFrame" in script
        assert "astype" in script or "to_datetime" in script
        mock_chat.assert_called_once()
    
    def test_extract_code_from_malformed_response(self, agent):
        """Test code extraction from malformed LLM response."""
        malformed_response = """
Here's the code:
```python
import pandas as pd
# Code starts but block never closes...
"""
        
        code = agent._extract_code_from_response(malformed_response)
        
        # Should handle gracefully
        assert code is None or "import pandas" in code


class TestCodeExtractionEdgeCases:
    """Test edge cases in code extraction."""
    
    @pytest.fixture
    def ingestion_agent(self):
        """Create ingestion agent with mocked LLM client."""
        with patch('src.agents.mas.base_role.get_llm_client'):
            return IngestionSpecialistAgent()
    
    def test_multiple_code_blocks(self, ingestion_agent):
        """Test extraction when response has multiple code blocks."""
        response = """
First, here's a helper function:
```python
def helper():
    pass
```

And here's the main script:
```python
import requests
data = requests.get('url').json()
```
"""
        
        code = ingestion_agent._extract_code_from_response(response)
        
        # Should extract first valid block
        assert code is not None
        assert "def helper" in code or "import requests" in code
    
    def test_code_block_with_extra_whitespace(self, ingestion_agent):
        """Test extraction with extra whitespace."""
        response = """
```python

import requests


data = requests.get('url')


```
"""
        
        code = ingestion_agent._extract_code_from_response(response)
        
        assert code is not None
        assert "import requests" in code
    
    def test_nested_code_markers(self, ingestion_agent):
        """Test extraction with nested code markers in strings."""
        response = """
```python
import requests
# This comment mentions ```python but it's in a comment
code = "```python\\nprint('hello')\\n```"
```
"""
        
        code = ingestion_agent._extract_code_from_response(response)
        
        assert code is not None
        assert "import requests" in code
