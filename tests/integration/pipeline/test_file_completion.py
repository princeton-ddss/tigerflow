"""Tests for how Pipeline cleans up files that finished every task.

A worker that sees a symlink still present but the task output already gone
treats the file as unprocessed and redoes it, so cleanup must never leave a
completed file in that state.
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from tigerflow.pipeline import Pipeline

from .helpers import PipelineFactory, stage_file, task_spec, write_output


def setup_completed_file(pipeline: Pipeline, input_dir: Path, file_id: str):
    """Stage a file and write its task output so it looks fully processed."""
    stage_file(pipeline, input_dir, file_id)
    write_output(pipeline, file_id)


class TestCleanupOrdering:
    """Cleanup must never leave a file in a state a worker would redo."""

    @pytest.mark.parametrize("delete_input", [False, True])
    def test_no_unsafe_intermediate_state(
        self,
        pipeline_factory: PipelineFactory,
        input_dir: Path,
        delete_input: bool,
    ):
        """Check the cleanup invariants at the moment each unlink happens.

        Asserting the invariants rather than the call sequence keeps the test
        green for any reordering that is still safe, and makes a failure point
        at the hazard instead of at a position in a list.
        """
        pipeline = pipeline_factory(delete_input=delete_input)
        setup_completed_file(pipeline, input_dir, "f1")

        finished_file = pipeline._finished_dir / "f1.txt"
        symlink_file = pipeline._symlinks_dir / "f1.txt"
        output_file = pipeline._config.tasks[0].output_dir / "f1.txt"

        original_unlink = Path.unlink
        checked = set()

        def checked_unlink(self, *args, **kwargs):
            if self == symlink_file:
                # A worker that finds no marker treats the file as unprocessed
                assert finished_file.exists(), ".finished must be recorded first"
                checked.add("symlink")
            elif self == output_file:
                # A live symlink with no output also makes a worker redo the file
                assert not symlink_file.exists(), "Symlink must be removed first"
                checked.add("output")
            return original_unlink(self, *args, **kwargs)

        with patch.object(Path, "unlink", checked_unlink):
            pipeline._handle_processed_files()

        # Without this, a cleanup that skips an unlink entirely would pass
        assert checked == {"symlink", "output"}

    def test_output_unlink_tolerates_missing_file(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Output deletion should not raise if the file disappears mid-cleanup.

        Simulates another process deleting the output between detection and
        the unlink call.
        """
        pipeline = pipeline_factory()
        setup_completed_file(pipeline, input_dir, "f1")

        task = pipeline._config.tasks[0]
        output_file = task.output_dir / "f1.txt"

        original_unlink = Path.unlink
        deleted_early = False

        def unlink_with_race(self, *args, **kwargs):
            nonlocal deleted_early
            if self == output_file and not deleted_early:
                deleted_early = True
                original_unlink(self)  # Another process wins the race
            return original_unlink(self, *args, **kwargs)

        with patch.object(Path, "unlink", unlink_with_race):
            pipeline._handle_processed_files()

        assert deleted_early
        assert (pipeline._finished_dir / "f1.txt").exists()
        assert not (pipeline._symlinks_dir / "f1.txt").exists()


class TestCleanupState:
    """Filesystem state after a file completes every task."""

    def test_state_after_processing(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Completion records the marker and clears staged, output, and input."""
        pipeline = pipeline_factory(delete_input=True)
        task = pipeline._config.tasks[0]

        setup_completed_file(pipeline, input_dir, "f1")

        pipeline._handle_processed_files()

        assert (pipeline._finished_dir / "f1.txt").exists()
        assert not (pipeline._symlinks_dir / "f1.txt").exists()
        assert not (task.output_dir / "f1.txt").exists()
        assert not (input_dir / "f1.txt").exists()

    def test_preserves_input_when_not_deleting(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        pipeline = pipeline_factory(delete_input=False)
        task = pipeline._config.tasks[0]

        setup_completed_file(pipeline, input_dir, "f1")

        pipeline._handle_processed_files()

        assert (pipeline._finished_dir / "f1.txt").exists()
        assert not (pipeline._symlinks_dir / "f1.txt").exists()
        assert not (task.output_dir / "f1.txt").exists()
        assert (input_dir / "f1.txt").exists()

    def test_multiple_files(self, pipeline_factory: PipelineFactory, input_dir: Path):
        pipeline = pipeline_factory()
        task = pipeline._config.tasks[0]

        for i in range(3):
            setup_completed_file(pipeline, input_dir, f"file{i}")

        pipeline._handle_processed_files()

        for i in range(3):
            assert (pipeline._finished_dir / f"file{i}.txt").exists()
            assert not (pipeline._symlinks_dir / f"file{i}.txt").exists()
            assert not (task.output_dir / f"file{i}.txt").exists()

    def test_state_after_processing_with_differing_exts(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Completion derives the file ID from `output_ext` but names files with `input_ext`.

        From one ID this step writes the finished marker and removes the
        symlink and input file, all at the root `input_ext`, while the task
        output it deletes carries `output_ext`.
        """
        pipeline = pipeline_factory(
            [task_spec(input_ext=".txt", output_ext=".json")], delete_input=True
        )
        task = pipeline._config.tasks[0]

        setup_completed_file(pipeline, input_dir, "f1")
        assert (task.output_dir / "f1.json").exists()

        pipeline._handle_processed_files()

        assert (pipeline._finished_dir / "f1.txt").exists()
        assert not (pipeline._symlinks_dir / "f1.txt").exists()
        assert not (input_dir / "f1.txt").exists()
        assert not (task.output_dir / "f1.json").exists()
