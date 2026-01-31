"""CLI commands for managing and discovering tasks."""

import importlib
import pkgutil
from importlib.metadata import entry_points
from typing import Annotated

import typer

app = typer.Typer()


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


@app.command(name="list")
def list_tasks(
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Show full module paths"),
    ] = False,
):
    """
    List available tasks (built-in and installed).
    """
    builtin = _get_builtin_tasks()
    installed = _get_installed_tasks()

    if not builtin and not installed:
        print("No tasks found.")
        return

    if builtin:
        print("Built-in tasks:")
        for name, module_name in sorted(builtin):
            desc = _get_task_description(module_name)
            if verbose:
                line = f"  {name}: {module_name}"
            else:
                line = f"  {name}"
            if desc:
                line += f" - {desc}"
            print(line)

    if installed:
        if builtin:
            print()
        print("Installed tasks:")
        for name, module_path in sorted(installed):
            if verbose:
                print(f"  {name}: {module_path}")
            else:
                print(f"  {name}")


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
    elif task_name in installed:
        module_name = installed[task_name]
        source = "installed"
    else:
        print(f"Task '{task_name}' not found.")
        print("Run 'tigerflow tasks list' to see available tasks.")
        raise typer.Exit(1)

    print(f"Task: {task_name}")
    print(f"Source: {source}")
    print(f"Module: {module_name}")

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
