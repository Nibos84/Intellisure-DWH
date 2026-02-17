"""
Tests for JSON structured logging.
"""

import pytest
import json
import logging
from io import StringIO
from src.utils.json_logger import JsonFormatter, setup_json_logging, log_with_context


class TestJsonFormatter:
    """Test suite for JsonFormatter."""
    
    def test_basic_log_formatting(self):
        """Test basic log message formatting."""
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name='test_logger',
            level=logging.INFO,
            pathname='test.py',
            lineno=42,
            msg='Test message',
            args=(),
            exc_info=None
        )
        record.funcName = 'test_function'
        record.module = 'test_module'
        
        result = formatter.format(record)
        log_data = json.loads(result)
        
        assert log_data['level'] == 'INFO'
        assert log_data['logger'] == 'test_logger'
        assert log_data['message'] == 'Test message'
        assert log_data['module'] == 'test_module'
        assert log_data['function'] == 'test_function'
        assert log_data['line'] == 42
        assert 'timestamp' in log_data
    
    def test_log_with_extra_fields(self):
        """Test logging with extra context fields."""
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name='agent_logger',
            level=logging.INFO,
            pathname='agent.py',
            lineno=100,
            msg='Pipeline started',
            args=(),
            exc_info=None
        )
        record.funcName = 'execute'
        record.module = 'ingestion_specialist'
        record.pipeline_name = 'test_pipeline'
        record.agent_type = 'ingestion'
        record.status = 'running'
        
        result = formatter.format(record)
        log_data = json.loads(result)
        
        assert log_data['pipeline_name'] == 'test_pipeline'
        assert log_data['agent_type'] == 'ingestion'
        assert log_data['status'] == 'running'
    
    def test_log_with_exception(self):
        """Test logging with exception information."""
        formatter = JsonFormatter()
        
        try:
            raise ValueError("Test error")
        except ValueError:
            import sys
            exc_info = sys.exc_info()
            
            record = logging.LogRecord(
                name='error_logger',
                level=logging.ERROR,
                pathname='test.py',
                lineno=50,
                msg='An error occurred',
                args=(),
                exc_info=exc_info
            )
            record.funcName = 'error_function'
            record.module = 'error_module'
            
            result = formatter.format(record)
            log_data = json.loads(result)
            
            assert log_data['level'] == 'ERROR'
            assert 'exception' in log_data
            assert 'ValueError: Test error' in log_data['exception']
    
    def test_setup_json_logging(self):
        """Test JSON logging setup."""
        logger = setup_json_logging('test_json_logger', level=logging.DEBUG)
        
        assert logger.name == 'test_json_logger'
        assert logger.level == logging.DEBUG
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0].formatter, JsonFormatter)
    
    def test_log_with_context_helper(self):
        """Test log_with_context helper function."""
        # Capture log output
        stream = StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(JsonFormatter())
        
        logger = logging.getLogger('context_test_logger')
        logger.handlers = [handler]
        logger.setLevel(logging.INFO)
        
        log_with_context(
            logger,
            logging.INFO,
            'Pipeline completed',
            pipeline_name='test_pipeline',
            agent_type='transformation',
            duration_ms=1500
        )
        
        output = stream.getvalue()
        log_data = json.loads(output.strip())
        
        assert log_data['message'] == 'Pipeline completed'
        assert log_data['pipeline_name'] == 'test_pipeline'
        assert log_data['agent_type'] == 'transformation'
        assert log_data['duration_ms'] == 1500
    
    def test_timestamp_format(self):
        """Test timestamp is in ISO 8601 format."""
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='test.py',
            lineno=1,
            msg='Test',
            args=(),
            exc_info=None
        )
        record.funcName = 'test'
        record.module = 'test'
        
        result = formatter.format(record)
        log_data = json.loads(result)
        
        # Check timestamp ends with 'Z' (UTC)
        assert log_data['timestamp'].endswith('Z')
        # Check it's a valid ISO format
        from datetime import datetime
        datetime.fromisoformat(log_data['timestamp'].rstrip('Z'))
