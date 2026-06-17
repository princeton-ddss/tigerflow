import dataclasses
import importlib
import json
import os
import re
import subprocess
import sys
import tempfile
import traceback
from collections.abc import Callable
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from subprocess import TimeoutExpired
from types import SimpleNamespace

TEMP_FILE_PREFIX = ".~tf_"


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


def validate_callable_reference(ref: str) -> str:
    """
    Validate that a string is a valid callable reference in 'module:function' format.
    """
    parts = ref.split(":")
    if len(parts) != 2:
        raise ValueError(f"Callable reference must contain exactly one ':': {ref}")

    module_path, func_name = parts

    for part in module_path.split("."):
        if not part.isidentifier():
            raise ValueError(f"Invalid Python identifier '{part}' in: {ref}")

    if not func_name.isidentifier():
        raise ValueError(f"Invalid Python identifier '{func_name}' in: {ref}")

    return ref


def import_callable(ref: str) -> Callable:
    """
    Import a callable from a 'module:function' reference string.
    """
    module_path, func_name = ref.split(":")
    module = importlib.import_module(module_path)
    obj = getattr(module, func_name)
    if not callable(obj):
        raise TypeError(f"'{ref}' does not resolve to a callable")
    return obj


def validate_task_cli(module: str, *, timeout: int = 60):
    """Validate that the given module is a valid task CLI.

    A valid task CLI runs successfully with --help (exit code 0).
    Task subclasses using the built-in cli() method will have the
    required options (--input-dir, --input-ext, --output-dir, --output-ext).

    Parameters
    ----------
    module : str
        Either a file path ending in .py or a fully qualified module name
        (e.g., 'tigerflow.library.echo')

    Raises
    ------
    ValueError
        If the CLI exits with a non-zero code. The error message includes
        the stderr (or stdout) output from running ``--help``.
    TimeoutError
        If the CLI does not respond within the timeout.
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

    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise ValueError(
            f"Invalid task CLI '{module}' (exit code {result.returncode}):\n{detail}"
        )


def submit_to_slurm(script: str) -> int:
    result = subprocess.run(
        ["sbatch"],
        capture_output=True,
        input=script,
        text=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"sbatch failed (exit code {result.returncode}):\n{detail}")

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


def has_running_pid(pid_file: Path) -> bool:
    """
    Check if a PID file exists with a running process.

    Returns True if a process is already running, False otherwise.
    """
    pid = read_pid_file(pid_file)
    if pid is None:
        return False
    return is_process_running(pid)


@contextmanager
def atomic_write(filepath: str | os.PathLike[str]):
    """
    Context manager for atomic writing:
    1. Creates a temporary file with the target's suffix
       (so libraries that inspect the extension behave correctly)
       and a distinguishable prefix (so downstream tasks that
       match on extension alone won't pick it up prematurely)
    2. Yields its path for writing
    3. Atomically replaces the target file on success
    """
    filepath = Path(filepath)

    fd, temp_path = tempfile.mkstemp(
        prefix=TEMP_FILE_PREFIX,
        suffix=filepath.suffix,
        dir=filepath.parent,
    )
    os.close(fd)

    temp_path = Path(temp_path)

    try:
        yield temp_path
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    else:
        temp_path.replace(filepath)


@dataclasses.dataclass(slots=True)
class ErrorRecord:
    """Structured error record for JSON serialization.

    Represents the on-disk schema used by .err and .setup-failed files.
    """

    timestamp: str
    exception_type: str
    message: str
    traceback: str
    file: str | None = None

    @classmethod
    def from_exception(cls, file: str | None = None) -> "ErrorRecord":
        """Capture error details from the current exception context.

        Must be called from within an exception handler. *file* is the
        name of the input file being processed, if any; omit for errors
        not associated with a specific file (e.g. task setup failures).
        """
        exc_type, exc_value, _ = sys.exc_info()
        return cls(
            timestamp=datetime.now(timezone.utc).isoformat(),
            exception_type=exc_type.__name__ if exc_type else "Unknown",
            message=str(exc_value) if exc_value else "",
            traceback=traceback.format_exc(),
            file=file,
        )

    def write(self, path: Path) -> None:
        """Write error record as JSON to *path* using atomic write."""
        with atomic_write(path) as temp_path:
            with open(temp_path, "w") as f:
                json.dump(dataclasses.asdict(self), f, indent=2)

    @classmethod
    def read(cls, path: Path) -> "ErrorRecord":
        """Read error record from a JSON file.

        Raises `ValueError` if the file content is malformed or incomplete.
        """
        try:
            data = json.loads(path.read_text())
            return cls(**data)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError(f"invalid error record: {path}") from exc
