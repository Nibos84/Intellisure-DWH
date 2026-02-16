"""
Tests for execution timeout functionality.

Verifies that the timeout mechanism properly interrupts long-running code
and allows fast code to complete normally.
"""
import pytest
import platform
import time
from src.utils.execution import time_limit, TimeoutException


# Skip signal-based tests on Windows
is_windows = platform.system() == 'Windows'
skip_on_windows = pytest.mark.skipif(is_windows, reason="Signal-based timeout not supported on Windows")


@skip_on_windows
def test_timeout_enforced():
    """Test that timeout stops long-running code (Unix/Linux only)."""
    with pytest.raises(TimeoutException) as exc_info:
        with time_limit(1):
            time.sleep(5)
    
    assert "timed out after 1 seconds" in str(exc_info.value)


def test_timeout_allows_fast_code():
    """Test that timeout doesn't affect fast code."""
    with time_limit(5):
        time.sleep(0.1)
    # Should not raise


@skip_on_windows
def test_timeout_cleans_up_alarm():
    """Test that alarm is properly cleared after timeout context (Unix/Linux only)."""
    # First timeout context
    with time_limit(5):
        time.sleep(0.1)
    
    # Verify alarm was cleared - this should not timeout
    time.sleep(0.2)
    
    # Second timeout context should work independently
    with pytest.raises(TimeoutException):
        with time_limit(1):
            time.sleep(3)


@skip_on_windows
def test_timeout_with_exception():
    """Test that alarm is cleared even when exception occurs in context (Unix/Linux only)."""
    try:
        with time_limit(5):
            raise ValueError("Test exception")
    except ValueError:
        pass
    
    # Verify alarm was cleared - sleep should work
    time.sleep(0.2)
