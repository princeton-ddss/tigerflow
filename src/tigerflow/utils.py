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
            timeout=60,
        )
        return expected_phrase in result.stdout
    except TimeoutExpired:
        return False


def get_slurm_max_array_size() -> int:
    result = subprocess.run(
        ["scontrol", "show", "config"],
        capture_output=True,
        text=True,
    )

    match = re.search(r"MaxArraySize\s*=\s*(\d+)", result.stdout)
    if not match:
        raise RuntimeError("Could not find 'MaxArraySize' in Slurm configuration")

    return int(match.group(1))


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
