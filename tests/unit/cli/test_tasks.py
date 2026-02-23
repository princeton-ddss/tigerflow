import json

from typer.testing import CliRunner

from tigerflow.cli import app
from tigerflow.cli.tasks import (
    _get_builtin_tasks,
    _get_installed_tasks,
    _get_package_version,
    _get_task_description,
)

runner = CliRunner()


class TestGetBuiltinTasks:
    def test_returns_list(self):
        tasks = _get_builtin_tasks()
        assert isinstance(tasks, list)

    def test_includes_echo_task(self):
        tasks = _get_builtin_tasks()
        task_names = [name for name, _ in tasks]
        assert "echo" in task_names

    def test_returns_module_paths(self):
        tasks = _get_builtin_tasks()
        for name, module_path in tasks:
            assert module_path.startswith("tigerflow.library.")


class TestGetInstalledTasks:
    def test_returns_list(self):
        tasks = _get_installed_tasks()
        assert isinstance(tasks, list)


class TestGetTaskDescription:
    def test_returns_description_for_echo(self):
        desc = _get_task_description("tigerflow.library.echo")
        assert desc is not None
        assert "Echo" in desc or "echo" in desc.lower()

    def test_returns_none_for_nonexistent_module(self):
        desc = _get_task_description("nonexistent.module.that.does.not.exist")
        assert desc is None


class TestGetPackageVersion:
    def test_returns_version_for_tigerflow(self):
        version = _get_package_version("tigerflow.library.echo")
        assert version is not None
        # Version should be a valid semver-like string
        assert "." in version

    def test_returns_none_for_nonexistent_module(self):
        version = _get_package_version("nonexistent.module.xyz")
        assert version is None

    def test_returns_version_for_stdlib(self):
        # Standard library modules don't have versions
        version = _get_package_version("os.path")
        # May or may not return None depending on implementation
        # Just ensure it doesn't raise an exception
        assert version is None or isinstance(version, str)


class TestTasksListCommand:
    def test_list_command_succeeds(self):
        result = runner.invoke(app, ["tasks", "list"])
        assert result.exit_code == 0

    def test_list_shows_builtin_tasks(self):
        result = runner.invoke(app, ["tasks", "list"])
        assert "Built-in tasks:" in result.stdout
        assert "echo" in result.stdout

    def test_list_verbose_shows_module_paths(self):
        result = runner.invoke(app, ["tasks", "list", "-v"])
        assert "tigerflow.library.echo" in result.stdout

    def test_list_shows_version(self):
        result = runner.invoke(app, ["tasks", "list"])
        # Version should appear in parentheses after task name
        assert "(" in result.stdout and ")" in result.stdout

    def test_list_json_output(self):
        result = runner.invoke(app, ["tasks", "list", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert "builtin" in data
        assert "installed" in data
        assert isinstance(data["builtin"], list)

    def test_list_json_includes_task_details(self):
        result = runner.invoke(app, ["tasks", "list", "--json"])
        data = json.loads(result.stdout)
        # Find echo task in builtin
        echo_task = next((t for t in data["builtin"] if t["name"] == "echo"), None)
        assert echo_task is not None
        assert "module" in echo_task
        assert "version" in echo_task
        assert "description" in echo_task
        assert echo_task["module"] == "tigerflow.library.echo"

    def test_list_json_version_is_string_or_null(self):
        result = runner.invoke(app, ["tasks", "list", "--json"])
        data = json.loads(result.stdout)
        for task in data["builtin"]:
            assert task["version"] is None or isinstance(task["version"], str)


class TestTasksInfoCommand:
    def test_info_command_for_echo(self):
        result = runner.invoke(app, ["tasks", "info", "echo"])
        assert result.exit_code == 0
        assert "Task: echo" in result.stdout
        assert "Source: built-in" in result.stdout

    def test_info_shows_version(self):
        result = runner.invoke(app, ["tasks", "info", "echo"])
        assert "Version:" in result.stdout

    def test_info_shows_parameters(self):
        result = runner.invoke(app, ["tasks", "info", "echo"])
        assert "Parameters" in result.stdout
        assert "--prefix" in result.stdout

    def test_info_unknown_task_fails(self):
        result = runner.invoke(app, ["tasks", "info", "nonexistent_task"])
        assert result.exit_code == 1
        assert "not found" in result.stdout
