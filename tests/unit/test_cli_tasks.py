from typer.testing import CliRunner

from tigerflow.cli import app
from tigerflow.cli.tasks import (
    _get_builtin_tasks,
    _get_installed_tasks,
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


class TestTasksInfoCommand:
    def test_info_command_for_echo(self):
        result = runner.invoke(app, ["tasks", "info", "echo"])
        assert result.exit_code == 0
        assert "Task: echo" in result.stdout
        assert "Source: built-in" in result.stdout

    def test_info_shows_parameters(self):
        result = runner.invoke(app, ["tasks", "info", "echo"])
        assert "Parameters" in result.stdout
        assert "--prefix" in result.stdout

    def test_info_unknown_task_fails(self):
        result = runner.invoke(app, ["tasks", "info", "nonexistent_task"])
        assert result.exit_code == 1
        assert "not found" in result.stdout
