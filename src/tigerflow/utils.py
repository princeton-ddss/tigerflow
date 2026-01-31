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


def get_params_from_class(cls) -> dict:
    """
    Extract parameter definitions from a task's Params inner class.

    Returns a dict of {name: (type_hint, default)} where default is
    inspect.Parameter.empty if no default is provided.
    """
    import inspect
    from typing import get_type_hints

    if not hasattr(cls, "Params"):
        return {}

    params = {}
    hints = get_type_hints(cls.Params, include_extras=True)

    for name, type_hint in hints.items():
        if hasattr(cls.Params, name):
            default = getattr(cls.Params, name)
        else:
            default = inspect.Parameter.empty
        params[name] = (type_hint, default)

    return params


def build_cli(cls, base_main):
    """
    Wrap a base CLI main function to include custom Params as CLI options.

    Inspects cls.Params for additional parameters and creates a new function
    with a combined signature that Typer can use. Also filters out internal
    parameters (starting with _) that Typer cannot handle.
    """
    import inspect

    # Get the base function's signature, excluding internal params (like _params)
    base_sig = inspect.signature(base_main)
    base_params = [
        p for p in base_sig.parameters.values() if not p.name.startswith("_")
    ]

    params_spec = get_params_from_class(cls)
    if not params_spec:
        # No custom params, but still need to filter out internal params
        new_sig = base_sig.replace(parameters=base_params)

        def wrapper(*args, **kwargs):
            return base_main(*args, _params=None, **kwargs)

        wrapper.__signature__ = new_sig
        wrapper.__doc__ = base_main.__doc__
        return wrapper

    # Build new parameters from Params class
    custom_params = []
    for name, (type_hint, default) in params_spec.items():
        param = inspect.Parameter(
            name,
            inspect.Parameter.KEYWORD_ONLY,
            default=default,
            annotation=type_hint,
        )
        custom_params.append(param)

    # Combine base params with custom params
    new_params = base_params + custom_params
    new_sig = base_sig.replace(parameters=new_params)

    # Create wrapper that separates custom params
    custom_keys = set(params_spec.keys())

    def wrapper(*args, **kwargs):
        custom_values = {k: kwargs.pop(k) for k in list(kwargs) if k in custom_keys}
        return base_main(*args, _params=custom_values, **kwargs)

    wrapper.__signature__ = new_sig
    wrapper.__doc__ = base_main.__doc__

    return wrapper


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
