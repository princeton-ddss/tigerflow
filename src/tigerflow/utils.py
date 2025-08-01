import os
import re
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from subprocess import TimeoutExpired


def is_valid_cli(file: Path) -> bool:
    """
    Check if the given file is a Typer CLI application.

    Notes
    -----
    The current implementation is a bit hacky;
    replace it with a more robust one if found.
    """
    expected_phrase = f"Usage: {file.name} [OPTIONS]"
    try:
        result = subprocess.run(
            [sys.executable, str(file), "--help"],
            capture_output=True,
            text=True,
            timeout=1,
        )
        if result.stderr:
            raise Exception(result.stderr)
        return expected_phrase in result.stdout
    except TimeoutExpired:
        return False


def get_slurm_max_array_size() -> int:
    try:
        result = subprocess.run(
            ["scontrol", "show", "config"],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to run 'scontrol show config': {e.stderr}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error: {e}")

    match = re.search(r"MaxArraySize\s*=\s*(\d+)", result.stdout)
    if not match:
        raise RuntimeError("Could not find 'MaxArraySize' in Slurm configuration")
    max_array_size = int(match.group(1))

    return max_array_size


@contextmanager
def atomic_write(filepath: os.PathLike):
    """
    Context manager for atomic writing:
    1. Creates a temporary file
    2. Yields its path for writing
    3. Atomically replaces the target file on success
    """
    filepath = Path(filepath)

    fd, temp_path = tempfile.mkstemp(dir=filepath.parent)
    os.close(fd)

    temp_path = Path(temp_path)

    try:
        yield temp_path
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    else:
        temp_path.replace(filepath)
