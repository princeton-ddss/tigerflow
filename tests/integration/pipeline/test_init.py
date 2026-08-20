"""Tests for the cleanup Pipeline performs at construction.

A new Pipeline inherits whatever the previous run left behind, so __init__
reconciles that state: finished files lose their symlinks and task outputs,
broken symlinks are dropped, and partial or misnamed outputs are removed.
"""

from pathlib import Path

import pytest

from tigerflow.models import PipelineOutput
from tigerflow.utils import TEMP_FILE_PREFIX

from .helpers import DEFAULT_TASK_NAME, PipelineFactory, task_spec


def make_prior_run_layout(output_dir: Path) -> PipelineOutput:
    """Build the layout a prior run would have left, before Pipeline exists."""
    layout = PipelineOutput(output_dir)
    layout.symlinks.mkdir(parents=True, exist_ok=True)
    layout.finished.mkdir(parents=True, exist_ok=True)
    return layout


def make_task_output_dir(layout: PipelineOutput, name: str = DEFAULT_TASK_NAME) -> Path:
    path = layout.internal / name
    path.mkdir(parents=True, exist_ok=True)
    return path


class TestInitCleanup:
    """Leftover state from a prior run should be reconciled on construction."""

    def test_finished_file_with_lingering_symlink_and_outputs(
        self,
        pipeline_factory: PipelineFactory,
        input_dir: Path,
        output_dir: Path,
    ):
        """Finished file + leftover symlink + task output should all be cleaned."""
        input_file = input_dir / "doc1.txt"
        input_file.write_text("content")

        layout = make_prior_run_layout(output_dir)
        (layout.finished / "doc1.txt").touch()

        symlink_file = layout.symlinks / "doc1.txt"
        symlink_file.symlink_to(input_file)

        output_file = make_task_output_dir(layout) / "doc1.txt"
        output_file.write_text("stale output")

        pipeline_factory()

        assert not symlink_file.exists()
        assert not output_file.exists()

    def test_broken_symlink_with_orphaned_outputs(
        self,
        pipeline_factory: PipelineFactory,
        input_dir: Path,
        output_dir: Path,
    ):
        """Broken symlink (target deleted) + task outputs should be cleaned."""
        layout = make_prior_run_layout(output_dir)
        symlink_file = layout.symlinks / "gone.txt"
        symlink_file.symlink_to(input_dir / "gone.txt")

        output_file = make_task_output_dir(layout) / "gone.txt"
        output_file.write_text("orphaned")

        pipeline_factory()

        # `exists()` follows the link and its target never existed, so it would
        # pass without any cleanup; `is_symlink()` checks the link itself.
        assert not symlink_file.is_symlink()
        assert not output_file.exists()

    @pytest.mark.parametrize("delete_input", [False, True])
    def test_finished_input_follows_delete_input(
        self,
        pipeline_factory: PipelineFactory,
        input_dir: Path,
        output_dir: Path,
        delete_input: bool,
    ):
        """A finished file's input is deleted only when delete_input is set."""
        input_file = input_dir / "doc1.txt"
        input_file.write_text("content")

        layout = make_prior_run_layout(output_dir)
        (layout.finished / "doc1.txt").touch()

        pipeline_factory(delete_input=delete_input)

        assert input_file.exists() is not delete_input

    def test_non_symlink_junk_in_symlinks_dir(
        self, pipeline_factory: PipelineFactory, output_dir: Path
    ):
        layout = make_prior_run_layout(output_dir)
        junk_file = layout.symlinks / "not_a_symlink.txt"
        junk_file.write_text("junk")

        pipeline_factory()

        assert not junk_file.exists()

    def test_valid_symlink_preserved(
        self,
        pipeline_factory: PipelineFactory,
        input_dir: Path,
        output_dir: Path,
    ):
        input_file = input_dir / "active.txt"
        input_file.write_text("in progress")

        layout = make_prior_run_layout(output_dir)
        symlink_file = layout.symlinks / "active.txt"
        symlink_file.symlink_to(input_file)

        pipeline_factory()

        assert symlink_file.is_symlink()

    def test_temp_files_with_output_ext_removed(
        self, pipeline_factory: PipelineFactory, output_dir: Path
    ):
        layout = make_prior_run_layout(output_dir)
        task_output_dir = make_task_output_dir(layout)

        temp_file = task_output_dir / f"{TEMP_FILE_PREFIX}abc123.txt"
        temp_file.write_text("partial")
        output_file = task_output_dir / "doc1.txt"
        output_file.write_text("complete")

        pipeline_factory()

        assert not temp_file.exists()
        assert output_file.exists()

    def test_extensionless_file_removed(
        self, pipeline_factory: PipelineFactory, output_dir: Path
    ):
        """A file not matching the output extension should be removed.

        Cleanup keys off the output extension rather than the temp prefix, so
        an extensionless file is discarded even without the prefix.
        """
        layout = make_prior_run_layout(output_dir)
        task_output_dir = make_task_output_dir(layout)

        extensionless_file = task_output_dir / "somefile"
        extensionless_file.write_text("not a temp file")

        pipeline_factory()

        assert not extensionless_file.exists()

    @pytest.mark.parametrize("delete_input", [False, True])
    def test_orphaned_outputs_cleaned_at_task_ext(
        self,
        pipeline_factory: PipelineFactory,
        input_dir: Path,
        output_dir: Path,
        delete_input: bool,
    ):
        """Orphan cleanup resolves each path at the extension that names it.

        File IDs come from the finished dir, where names carry the root input
        extension. From one ID this step deletes two differently named files:
        the task output at `output_ext`, and the input file at `input_ext`.
        """
        input_file = input_dir / "doc1.txt"
        input_file.write_text("content")

        layout = make_prior_run_layout(output_dir)
        (layout.finished / "doc1.txt").touch()

        symlink_file = layout.symlinks / "doc1.txt"
        symlink_file.symlink_to(input_file)

        output_file = make_task_output_dir(layout) / "doc1.json"
        output_file.write_text("stale output")

        pipeline_factory(
            [task_spec(input_ext=".txt", output_ext=".json")],
            delete_input=delete_input,
        )

        assert not symlink_file.exists()
        assert not output_file.exists()
        assert input_file.exists() is not delete_input
