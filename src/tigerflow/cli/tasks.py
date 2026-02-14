"""CLI commands for managing and discovering tasks."""

import importlib
import json
import pkgutil
from importlib.metadata import entry_points, packages_distributions, version
from typing import Annotated

import typer
from rich import print

app = typer.Typer()


@app.command(name="list")
def list_tasks(
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Show full module paths"),
    ] = False,
    output_json: Annotated[
        bool,
        typer.Option("--json", help="Output in JSON format"),
    ] = False,
):
    """
    List available tasks (built-in and installed).
    """
    builtin = _get_builtin_tasks()
    installed = _get_installed_tasks()

    if output_json:
        _list_tasks_json(builtin, installed)
    else:
        _list_tasks_rich(builtin, installed, verbose)


def _list_tasks_json(builtin: list[tuple[str, str]], installed: list[tuple[str, str]]):
    """Output task list in JSON format."""
    data = {
        "builtin": [
            {
                "name": name,
                "module": module_name,
                "version": _get_package_version(module_name),
                "description": _get_task_description(module_name),
            }
            for name, module_name in sorted(builtin)
        ],
        "installed": [
            {
                "name": name,
                "module": module_path,
                "version": _get_package_version(module_path.split(":")[0]),
                "description": _get_task_description(module_path),
            }
            for name, module_path in sorted(installed)
        ],
    }
    typer.echo(json.dumps(data, indent=2))


def _list_tasks_rich(
    builtin: list[tuple[str, str]],
    installed: list[tuple[str, str]],
    verbose: bool,
):
    """Output task list with rich formatting."""
    if not builtin and not installed:
        print("No tasks found.")
        return

    if builtin:
        print("Built-in tasks:")
        for name, module_name in sorted(builtin):
            desc = _get_task_description(module_name)
            ver = _get_package_version(module_name)
            if verbose:
                line = f"  {name}: {module_name}"
            else:
                line = f"  {name}"
            if ver:
                line += f" ({ver})"
            if desc:
                line += f" - {desc}"
            print(line)

    if installed:
        if builtin:
            print()
        print("Installed tasks:")
        for name, module_path in sorted(installed):
            ver = _get_package_version(module_path.split(":")[0])
            if verbose:
                line = f"  {name}: {module_path}"
            else:
                line = f"  {name}"
            if ver:
                line += f" ({ver})"
            print(line)


@app.command(name="info")
def task_info(
    task_name: Annotated[
        str,
        typer.Argument(help="Name of the task to get info about"),
    ],
):
    """
    Show detailed information about a task.
    """
    # Check built-in tasks first
    builtin = {name: module for name, module in _get_builtin_tasks()}
    installed = {name: module for name, module in _get_installed_tasks()}

    if task_name in builtin:
        module_name = builtin[task_name]
        source = "built-in"
        module_version = _get_package_version(module_name)
    elif task_name in installed:
        module_name = installed[task_name]
        source = "installed"
        module_version = _get_package_version(module_name)
    else:
        print(f"Task '{task_name}' not found.")
        print("Run 'tigerflow tasks list' to see available tasks.")
        raise typer.Exit(1)

    print(f"Task: {task_name}")
    print(f"Source: {source}")
    print(f"Module: {module_name}")
    print(f"Version: {module_version}")

    # Try to get the task class and its Params
    try:
        module_path, class_name = _parse_module_path(module_name)
        module = importlib.import_module(module_path)
        if module.__doc__:
            print(f"\nDescription:\n{module.__doc__.strip()}")

        # Find task classes with Params
        # If class name was specified in entry point, use it directly
        if class_name and hasattr(module, class_name):
            task_classes = [(class_name, getattr(module, class_name))]
        else:
            task_classes = [
                (name, attr)
                for name in dir(module)
                if isinstance(attr := getattr(module, name), type)
                and hasattr(attr, "Params")
                and hasattr(attr, "cli")
            ]

        for attr_name, attr in task_classes:
            params_class = getattr(attr, "Params", None)
            if params_class:
                annotations = getattr(params_class, "__annotations__", {})
                if annotations:
                    print(f"\nParameters for {attr_name}:")
                    for param_name, param_type in annotations.items():
                        default = getattr(params_class, param_name, "(required)")
                        print(f"  --{param_name.replace('_', '-')}: {default}")
            break
    except Exception as e:
        print(f"\nCould not load task details: {e}")


def _get_package_version(module_name: str) -> str | None:
    """Get the version of the package that provides a module."""
    try:
        # Get the top-level package name
        top_level = module_name.split(".")[0]
        # Try direct version lookup first (works for most packages)
        return version(top_level)
    except Exception:
        pass
    try:
        # Fall back to packages_distributions mapping
        pkg_dist = packages_distributions()
        top_level = module_name.split(".")[0]
        if top_level in pkg_dist:
            dist_name = pkg_dist[top_level][0]
            return version(dist_name)
    except Exception:
        pass
    return None


def _get_builtin_tasks() -> list[tuple[str, str]]:
    """Get list of built-in tasks from tigerflow.library."""
    tasks = []
    try:
        import tigerflow.library as library

        for module_info in pkgutil.iter_modules(library.__path__):
            if not module_info.name.startswith("_"):
                module_name = f"tigerflow.library.{module_info.name}"
                tasks.append((module_info.name, module_name))
    except ImportError:
        pass
    return tasks


def _get_installed_tasks() -> list[tuple[str, str]]:
    """Get list of tasks installed via entry points."""
    tasks = []
    try:
        eps = entry_points(group="tigerflow.tasks")
        for ep in eps:
            tasks.append((ep.name, ep.value))
    except Exception:
        pass
    return tasks


def _parse_module_path(module_path: str) -> tuple[str, str | None]:
    """Parse module path that may include class name (e.g., 'module.path:ClassName')."""
    if ":" in module_path:
        module_name, class_name = module_path.split(":", 1)
        return module_name, class_name
    return module_path, None


def _get_task_description(module_path: str) -> str | None:
    """Try to get a task's description from its docstring."""
    try:
        module_name, _ = _parse_module_path(module_path)
        module = importlib.import_module(module_name)
        if module.__doc__:
            # Get first non-empty line of docstring
            for line in module.__doc__.strip().split("\n"):
                line = line.strip()
                if line:
                    return line
    except Exception:
        pass
    return None
