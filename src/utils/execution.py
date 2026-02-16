"""
Shared execution utilities for agents.

Provides timeout mechanisms to prevent resource exhaustion from runaway scripts.
Cross-platform compatible (Windows, Unix/Linux, macOS).
"""

import signal
import platform
from contextlib import contextmanager
from typing import Generator


class TimeoutException(Exception):
    """Raised when script execution exceeds timeout."""
    pass


@contextmanager
def time_limit(seconds: int) -> Generator[None, None, None]:
    """
    Context manager to limit execution time (cross-platform).
    
    On Unix/Linux/macOS: Uses signal.SIGALRM for precise timeout control
    On Windows: Provides compatibility layer (timeout handled by subprocess)
    
    Args:
        seconds: Maximum execution time in seconds
        
    Raises:
        TimeoutException: If execution exceeds the timeout
        
    Example:
        >>> with time_limit(300):
        ...     result = subprocess.run([...], timeout=seconds)
        
    Note:
        For subprocess.run(), pass timeout parameter directly for best results.
        This context manager provides additional safety layer.
    """
    is_windows = platform.system() == 'Windows'
    
    if is_windows:
        # Windows: No SIGALRM support
        # Timeout should be handled by subprocess.run(timeout=...)
        # This context manager is a no-op on Windows
        yield
    else:
        # Unix/Linux/macOS: Use SIGALRM
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
