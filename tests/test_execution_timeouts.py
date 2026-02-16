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


# Windows-specific tests
skip_on_unix = pytest.mark.skipif(not is_windows, reason="Windows-specific test")


@skip_on_unix
def test_windows_subprocess_timeout_enforced():
    """Test that subprocess timeout works on Windows."""
    import subprocess
    import sys
    
    with pytest.raises(subprocess.TimeoutExpired):
        subprocess.run(
            [sys.executable, '-c', 'import time; time.sleep(10)'],
            timeout=1,
            capture_output=True
        )


@skip_on_unix
def test_windows_time_limit_is_noop():
    """Test that time_limit context manager is a no-op on Windows."""
    import subprocess
    import sys
    
    # time_limit should not raise on Windows, even with long-running code
    # The timeout is handled by subprocess.run() timeout parameter
    start = time.time()
    timeout_seconds = 2
    try:
        with time_limit(1):  # This is a no-op on Windows
            # This will timeout via subprocess, not time_limit
            subprocess.run(
                [sys.executable, '-c', 'import time; time.sleep(5)'],
                timeout=timeout_seconds,  # subprocess timeout will kick in
                capture_output=True
            )
    except subprocess.TimeoutExpired:
        # Expected: subprocess timeout, not TimeoutException
        elapsed = time.time() - start
        # Allow 0.5s margin before timeout for process startup, 1s after for cleanup
        min_elapsed = timeout_seconds - 0.5
        max_elapsed = timeout_seconds + 1
        assert min_elapsed < elapsed < max_elapsed, \
            f"Expected ~{timeout_seconds}s timeout (±0.5-1s margin), got {elapsed:.2f}s"
    except TimeoutException:
        pytest.fail("time_limit should be no-op on Windows, not raise TimeoutException")


def test_cross_platform_timeout_allows_fast_code():
    """Test that fast code completes successfully on all platforms."""
    import subprocess
    import sys
    
    # Should work on both Windows and Unix
    with time_limit(5):
        result = subprocess.run(
            [sys.executable, '-c', 'print("success")'],
            timeout=5,
            capture_output=True,
            text=True
        )
    
    assert result.returncode == 0
    assert "success" in result.stdout
