"""Shared fixtures for integration tests."""

from pathlib import Path

import pytest


@pytest.fixture
def input_files(tmp_dirs):
    """Create sample input files."""
    input_dir, _ = tmp_dirs
    files = []
    for i, content in enumerate(["hello world", "foo bar", "test content"]):
        f = input_dir / f"file{i}.txt"
        f.write_text(content)
        files.append(f)
    return files
