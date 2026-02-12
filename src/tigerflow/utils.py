import os
import re
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from subprocess import TimeoutExpired
from types import SimpleNamespace


def get_version() -> str:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("tigerflow")
    except PackageNotFoundError:
        return "unknown"


def validate_file_ext(ext: str) -> str:
    """
    Return the string if it is a valid file extension.
    """
    if not re.fullmatch(r"(\.[a-zA-Z0-9_]+)+", ext):
        raise ValueError(f"Invalid file extension: {ext}")
    if ext.lower().endswith(".err"):
        raise ValueError(f"'.err' extension is reserved: {ext}")
    return ext


def is_valid_module_cli(file: Path, *, timeout: int = 60) -> bool:
    required_options = ["--input-dir", "--input-ext", "--output-dir", "--output-ext"]
    try:
        result = subprocess.run(
            [sys.executable, str(file), "--help"],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except TimeoutExpired:
        raise TimeoutError(f"CLI validation timed out after {timeout}s: {file}")

    return result.returncode == 0 and all(
        opt in result.stdout for opt in required_options
    )


def is_valid_library_cli(module_name: str, *, timeout: int = 60) -> bool:
    """
    Check if the given module name is a valid Typer CLI application.

    Parameters
    ----------
    module_name : str
        Fully qualified module name (e.g., 'tigerflow.library.echo')
    """
    try:
        result = subprocess.run(
            [sys.executable, "-m", module_name, "--help"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        # Check for Typer CLI signature: "Usage: ... [OPTIONS]"
        return "[OPTIONS]" in result.stdout and result.returncode == 0
    except TimeoutExpired:
        return False


def submit_to_slurm(script: str) -> int:
    result = subprocess.run(
        ["sbatch"],
        capture_output=True,
        check=True,
        input=script,
        text=True,
    )

    match = re.search(r"Submitted batch job (\d+)", result.stdout)
    if not match:
        raise ValueError("Failed to extract job ID from sbatch output")
    job_id = int(match.group(1))

    return job_id


class SetupContext(SimpleNamespace):
    """
    Namespace for user-defined setup variables.

    It can be frozen for read-only access.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._frozen = False

    def __setattr__(self, key, value):
        if getattr(self, "_frozen", False):
            raise AttributeError(
                f"Cannot modify frozen SetupContext: tried to set '{key}'"
            )
        super().__setattr__(key, value)

    def __delattr__(self, key):
        if getattr(self, "_frozen", False):
            raise AttributeError(
                f"Cannot modify frozen SetupContext: tried to delete '{key}'"
            )
        super().__delattr__(key)

    def freeze(self):
        self._frozen = True


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
