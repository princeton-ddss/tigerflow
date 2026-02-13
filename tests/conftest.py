from pathlib import Path

import pytest


@pytest.fixture
def tasks_dir() -> Path:
    """Path to sample task modules used in tests."""
    return Path(__file__).parent / "tasks"


@pytest.fixture
def tmp_module(tmp_path: Path) -> Path:
    """Create a temporary Python module file for testing."""
    module = tmp_path / "task_module.py"
    module.write_text("# test module\n")
    return module


@pytest.fixture
def tmp_dirs(tmp_path: Path) -> tuple[Path, Path]:
    """Create temporary input and output directories."""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    return input_dir, output_dir
