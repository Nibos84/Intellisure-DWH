"""
JSON structured logging formatter for better observability.

Provides structured JSON logs with consistent fields for monitoring and analysis.
"""

import logging
import json
from datetime import datetime
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.
    
    Outputs logs as JSON with consistent fields:
    - timestamp: ISO 8601 timestamp
    - level: Log level (INFO, ERROR, etc.)
    - logger: Logger name
    - message: Log message
    - module: Module name
    - function: Function name
    - line: Line number
    - exception: Exception traceback (if present)
    - extra: Any extra fields passed to logger
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data: Dict[str, Any] = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields (e.g., pipeline_name, agent_type)
        if hasattr(record, 'pipeline_name'):
            log_data['pipeline_name'] = record.pipeline_name
        if hasattr(record, 'agent_type'):
            log_data['agent_type'] = record.agent_type
        if hasattr(record, 'status'):
            log_data['status'] = record.status
        if hasattr(record, 'duration_ms'):
            log_data['duration_ms'] = record.duration_ms
        
        return json.dumps(log_data)


def setup_json_logging(logger_name: str = None, level: int = logging.INFO) -> logging.Logger:
    """
    Setup JSON logging for a logger.
    
    Args:
        logger_name: Name of logger (None for root logger)
        level: Logging level
        
    Returns:
        Configured logger with JSON formatter
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # Add JSON handler
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    
    return logger


def log_with_context(logger: logging.Logger, level: int, message: str, **context):
    """
    Log message with additional context fields.
    
    Args:
        logger: Logger instance
        level: Log level (logging.INFO, logging.ERROR, etc.)
        message: Log message
        **context: Additional context fields (pipeline_name, agent_type, etc.)
    """
    extra = {k: v for k, v in context.items()}
    logger.log(level, message, extra=extra)
