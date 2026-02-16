"""
Shared execution utilities for agents.

Provides timeout mechanisms to prevent resource exhaustion from runaway scripts.
"""
import signal
from contextlib import contextmanager
from typing import Generator


class TimeoutException(Exception):
    """Raised when script execution exceeds timeout."""
    pass


@contextmanager
def time_limit(seconds: int) -> Generator[None, None, None]:
    """
    Context manager to limit execution time.
    
    Uses signal.SIGALRM to interrupt execution after the specified timeout.
    This is a Unix-specific implementation and will not work on Windows.
    
    Args:
        seconds: Maximum execution time in seconds
        
    Raises:
        TimeoutException: If execution exceeds the timeout
        
    Example:
        >>> with time_limit(300):
        ...     long_running_operation()
        
    Note:
        This uses SIGALRM which is Unix-specific. For Windows compatibility,
        consider using threading.Timer or multiprocessing with timeout.
    """
    def signal_handler(signum, frame):
        raise TimeoutException(f"Execution timed out after {seconds} seconds")
    
    # Set the signal handler and a timeout alarm
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        # Disable the alarm
        signal.alarm(0)
