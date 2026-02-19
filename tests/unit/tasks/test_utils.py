import json
import re
from io import StringIO

import pytest
from loguru import logger

from tigerflow.tasks.utils import log_metrics


@pytest.fixture
def capture_logs():
    """Fixture to capture loguru output including METRICS level."""
    output = StringIO()
    handler_id = logger.add(output, format="{message}", level="INFO")
    yield output
    logger.remove(handler_id)


class TestLogMetrics:
    def test_success_status(self, capture_logs):
        """Test that successful execution logs status='success'."""
        with log_metrics("test.txt"):
            pass

        log_output = capture_logs.getvalue()
        match = re.search(r"\{.*\}", log_output)
        assert match, f"No JSON found in output: {log_output}"
        data = json.loads(match.group())

        assert data["file"] == "test.txt"
        assert data["status"] == "success"
        assert "started_at" in data
        assert "finished_at" in data

    def test_error_status_on_exception(self, capture_logs):
        """Test that exceptions result in status='error'."""
        with pytest.raises(ValueError):
            with log_metrics("test.txt"):
                raise ValueError("test error")

        log_output = capture_logs.getvalue()
        match = re.search(r"\{.*\}", log_output)
        assert match
        data = json.loads(match.group())

        assert data["status"] == "error"

    def test_error_status_set_manually(self, capture_logs):
        """Test that status can be set manually via yielded dict."""
        with log_metrics("test.txt") as metrics:
            metrics["status"] = "error"

        log_output = capture_logs.getvalue()
        match = re.search(r"\{.*\}", log_output)
        assert match
        data = json.loads(match.group())

        assert data["status"] == "error"

    def test_timestamps_are_iso_format(self, capture_logs):
        """Test that timestamps are valid ISO format."""
        from datetime import datetime

        with log_metrics("test.txt"):
            pass

        log_output = capture_logs.getvalue()
        match = re.search(r"\{.*\}", log_output)
        data = json.loads(match.group())

        # Should parse without error
        started = datetime.fromisoformat(data["started_at"])
        finished = datetime.fromisoformat(data["finished_at"])

        # finished should be >= started
        assert finished >= started

    def test_duration_is_reasonable(self, capture_logs):
        """Test that duration between timestamps is reasonable."""
        import time
        from datetime import datetime

        with log_metrics("test.txt"):
            time.sleep(0.1)  # Sleep 100ms

        log_output = capture_logs.getvalue()
        match = re.search(r"\{.*\}", log_output)
        data = json.loads(match.group())

        started = datetime.fromisoformat(data["started_at"])
        finished = datetime.fromisoformat(data["finished_at"])
        duration_ms = (finished - started).total_seconds() * 1000

        # Should be at least 100ms (we slept that long)
        assert duration_ms >= 100
        # But not too long (allow some overhead)
        assert duration_ms < 500
