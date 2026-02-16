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


def is_valid_task_cli(module: str, *, timeout: int = 60) -> bool:
    """
    Check if the given module is a valid task CLI.

    A valid task CLI runs successfully with --help (exit code 0).
    Task subclasses using the built-in cli() method will have the
    required options (--input-dir, --input-ext, --output-dir, --output-ext).

    Parameters
    ----------
    module : str
        Either a file path ending in .py or a fully qualified module name
        (e.g., 'tigerflow.library.echo')
    """
    if module.endswith(".py"):
        args = [sys.executable, module, "--help"]
    else:
        args = [sys.executable, "-m", module, "--help"]

    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except TimeoutExpired:
        raise TimeoutError(f"CLI validation timed out after {timeout}s: {module}")

    return result.returncode == 0


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


def read_pid_file(pid_file: Path) -> int | None:
    """
    Read PID from a file.

    Returns None if file doesn't exist or contains invalid content.
    """
    if not pid_file.exists():
        return None
    try:
        return int(pid_file.read_text().strip())
    except (ValueError, OSError):
        return None


def is_process_running(pid: int) -> bool:
    """
    Check if a process with the given PID is running.

    Uses os.kill(pid, 0) which checks process existence without sending a signal.
    """
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # Process exists but we don't have permission


def check_and_cleanup_stale_pid(pid_file: Path) -> bool:
    """
    Check if a PID file exists with a running process.

    Returns True if a process is already running (caller should error out).
    Returns False if no process is running (stale file cleaned up if present).
    """
    pid = read_pid_file(pid_file)
    if pid is None:
        return False

    if is_process_running(pid):
        return True

    # Stale PID file - process is dead, clean it up
    pid_file.unlink(missing_ok=True)
    return False


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
