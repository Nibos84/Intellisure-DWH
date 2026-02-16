"""
Shared execution utilities for agents.

Provides timeout mechanisms to prevent resource exhaustion from runaway scripts.
Cross-platform compatible (Windows, Unix/Linux, macOS).

Platform-Specific Behavior:
- Unix/Linux/macOS: Uses signal.SIGALRM for OS-level timeout enforcement
- Windows: Relies on subprocess.run(timeout=...) parameter only
  
Note: On Windows, timeout enforcement depends entirely on subprocess.run() timeout
parameter. Complex scripts with child processes may not be reliably terminated.
"""

import signal
import platform
import logging
from contextlib import contextmanager
from typing import Generator

logger = logging.getLogger(__name__)


class TimeoutException(Exception):
    """Raised when script execution exceeds timeout."""
    pass


@contextmanager
def time_limit(seconds: int) -> Generator[None, None, None]:
    """
    Context manager to limit execution time (cross-platform).
    
    On Unix/Linux/macOS: Uses signal.SIGALRM for precise timeout control at OS level.
    On Windows: No-op context manager. Timeout must be enforced via subprocess.run(timeout=...).
    
    Args:
        seconds: Maximum execution time in seconds
        
    Raises:
        TimeoutException: If execution exceeds the timeout (Unix/Linux/macOS only)
        
    Example:
        >>> with time_limit(300):
        ...     result = subprocess.run([...], timeout=300)  # Pass timeout to subprocess!
        
    Important:
        - Always pass timeout parameter to subprocess.run() for Windows compatibility
        - On Windows, this context manager provides no timeout enforcement
        - On Unix/Linux/macOS, provides additional OS-level safety layer
        
    Security Note:
        Windows limitation: subprocess timeout may not terminate child processes spawned
        by the script. For enhanced security, validate generated scripts to prevent
        subprocess creation or use process group management.
    """
    is_windows = platform.system() == 'Windows'
    
    if is_windows:
        # Windows: No SIGALRM support
        # Timeout MUST be handled by subprocess.run(timeout=...) parameter
        # Log once per execution to make limitation visible
        logger.debug(
            f"time_limit context manager is no-op on Windows. "
            f"Relying on subprocess timeout={seconds}s for enforcement."
        )
        yield
    else:
        # Unix/Linux/macOS: Use SIGALRM for OS-level timeout
        def signal_handler(signum, frame):
            """Signal handler for SIGALRM."""
            raise TimeoutException(f"Execution timed out after {seconds} seconds")
        
        # Set the signal handler and a timeout alarm
        old_handler = signal.signal(signal.SIGALRM, signal_handler)
        signal.alarm(seconds)
        try:
            yield
        finally:
            # Disable the alarm and restore old handler
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
