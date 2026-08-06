"""Tests for selecting input files and exposing pipeline state to middleware.

Staging turns files in the input directory into symlinks the root tasks can
see. Middleware decides which candidates make the cut, and reads counts from
a StagingContext the pipeline assembles from the filesystem.
"""

from collections.abc import Callable
from pathlib import Path

import pytest

from tigerflow.pipeline import Pipeline
from tigerflow.staging import StagingContext

from .helpers import PipelineFactory, task_spec

DUPLICATING_STEP = {
    "kind": "callable",
    "function": f"{__name__}:duplicate_candidates",
}


def duplicate_candidates(candidates: list[Path], context: StagingContext) -> list[Path]:
    """Staging middleware that returns its first candidate twice."""
    return [candidates[0], candidates[0]] if candidates else []


class TestStagingSelection:
    """Which input files become staged symlinks."""

    def test_stages_matching_extension_only(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Only files matching the root input extension are staged."""
        pipeline = pipeline_factory()
        (input_dir / "keep.txt").write_text("yes")
        (input_dir / "skip.csv").write_text("no")

        pipeline._stage_new_files()

        staged = {f.name for f in pipeline._symlinks_dir.iterdir()}
        assert staged == {"keep.txt"}

    def test_ignores_directories(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """A directory whose name matches the input extension is not staged."""
        pipeline = pipeline_factory()
        (input_dir / "adir.txt").mkdir()

        pipeline._stage_new_files()

        assert list(pipeline._symlinks_dir.iterdir()) == []

    def test_staged_symlink_resolves_to_input(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """The staged entry is a symlink pointing at the original input file."""
        pipeline = pipeline_factory()
        source = input_dir / "a.txt"
        source.write_text("payload")

        pipeline._stage_new_files()

        symlink_file = pipeline._symlinks_dir / "a.txt"
        assert symlink_file.is_symlink()
        assert symlink_file.resolve() == source.resolve()
        assert symlink_file.read_text() == "payload"

    def test_middleware_limits_staging(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """A max_batch step caps how many files are staged per cycle."""
        pipeline = pipeline_factory(
            staging={"steps": [{"kind": "max_batch", "count": 2}]}
        )
        for i in range(5):
            (input_dir / f"file{i}.txt").write_text("x")

        pipeline._stage_new_files()

        assert len(list(pipeline._symlinks_dir.iterdir())) == 2

    def test_duplicate_candidates_propagate_error(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Middleware returning the same file twice fails staging.

        `_stage_new_files` symlinks each returned candidate without checking
        for duplicates, so the second symlink for a file collides. Failing
        here is correct: middleware that returns duplicates is buggy, and
        silently deduping would hide it. What is wrong is how far the failure
        spreads, which `test_duplicate_candidates_do_not_end_run` pins down.
        """
        pipeline = pipeline_factory(staging={"steps": [DUPLICATING_STEP]})
        (input_dir / "a.txt").write_text("payload")

        with pytest.raises(FileExistsError):
            pipeline._stage_new_files()

    @pytest.mark.xfail(
        reason="Middleware output is unvalidated and the resulting error "
        "propagates out of the tracking loop, terminating the entire pipeline",
        strict=True,
    )
    def test_duplicate_candidates_do_not_end_run(
        self,
        pipeline_factory: PipelineFactory,
        input_dir: Path,
        stop_after_one_cycle: Callable[[Pipeline], None],
    ):
        """A buggy staging step should cost one cycle, not the whole run.

        Middleware faults are already non-fatal in the case that was anticipated:
        `CallableMiddleware` catches every exception from the user callable,
        warns, and stages nothing that cycle. The gap is a callable that raises
        nothing itself and instead returns bad data.

        Duplicates are one such fault, and the one covered here; returning
        `None` or `list[str]` ends the run the same way, and a fabricated path
        creates a dangling symlink with no error at all.

        `stop_after_one_cycle` forces shutdown because today the error ends the
        run on its own; once fixed, an unbounded `run()` would spin until the
        idle timeout.
        """
        pipeline = pipeline_factory(staging={"steps": [DUPLICATING_STEP]})
        (input_dir / "a.txt").write_text("payload")
        stop_after_one_cycle(pipeline)

        pipeline.run()  # Must not raise


class TestStagingContext:
    """Counts handed to middleware must reflect real pipeline state."""

    def test_counts_waiting_files(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Unstaged input files are reported as waiting."""
        pipeline = pipeline_factory()
        for i in range(3):
            (input_dir / f"f{i}.txt").write_text("x")

        context = pipeline._build_staging_context()

        assert context.waiting == 3
        assert context.staged == 0
        assert context.completed == 0

    def test_counts_shift_after_staging(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Staging moves files from waiting to staged."""
        pipeline = pipeline_factory()
        (input_dir / "a.txt").write_text("x")

        pipeline._stage_new_files()
        context = pipeline._build_staging_context()

        assert context.waiting == 0
        assert context.staged == 1

    def test_counts_completed_files(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """A file that finished every task moves from the staged count to completed."""
        pipeline = pipeline_factory()
        (input_dir / "a.txt").write_text("x")
        pipeline._stage_new_files()

        task = pipeline._config.tasks[0]
        (task.output_dir / "a.txt").write_text("done")
        pipeline._handle_processed_files()

        context = pipeline._build_staging_context()

        assert context.completed == 1
        assert context.staged == 0

    def test_staged_count_excludes_failures(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Files that errored are counted as failed, not staged."""
        pipeline = pipeline_factory()
        (input_dir / "a.txt").write_text("x")
        pipeline._stage_new_files()

        task = pipeline._config.tasks[0]
        (task.output_dir / "a.err").write_text("boom")
        pipeline._report_failed_files()

        context = pipeline._build_staging_context()

        assert context.failed == 1
        assert context.staged == 0

    def test_max_staged_respects_capacity(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """A max_staged step counts files already in flight, not just this batch."""
        pipeline = pipeline_factory(
            staging={"steps": [{"kind": "max_staged", "count": 2}]}
        )
        for i in range(5):
            (input_dir / f"f{i}.txt").write_text("x")

        pipeline._stage_new_files()
        first_batch = {f.name for f in pipeline._symlinks_dir.iterdir()}
        assert len(first_batch) == 2

        pipeline._stage_new_files()
        staged = {f.name for f in pipeline._symlinks_dir.iterdir()}
        assert staged == first_batch, (
            "Capacity is already full, so no further file should be staged"
        )

    def test_counts_stay_consistent_in_mixed_state(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """All four counts hold together when staged, completed, and failed coexist.

        The tests above each set up one state at a time, so this is the only
        place `waiting`, `staged`, `completed`, and `failed` are all pinned
        against each other. Four files are staged, one completes, one fails:
        completion removes a symlink and failure does not, so `staged` is 3
        symlinks minus 1 recorded failure. The counts are deliberately unequal
        because one file per state lets several wrong formulas land on the
        right answer.
        """
        pipeline = pipeline_factory()
        for i in range(4):
            (input_dir / f"f{i}.txt").write_text("x")
        pipeline._stage_new_files()

        task = pipeline._config.tasks[0]
        (task.output_dir / "f0.txt").write_text("done")
        (task.output_dir / "f1.err").write_text("boom")
        pipeline._report_failed_files()
        pipeline._handle_processed_files()

        context = pipeline._build_staging_context()

        counts = (context.waiting, context.staged, context.completed, context.failed)
        assert counts == (0, 2, 1, 1)

    @pytest.mark.xfail(
        reason="Failures are subtracted as a per-task total, so a file failing "
        "in two fan-out tasks is discounted twice and staged undercounts the "
        "files still live",
        strict=True,
    )
    def test_staged_discounts_each_failed_file_once(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """A file failing in several tasks must free one staging slot, not one per task.

        Excluding failures from `staged` is intended: a file that can no longer
        complete should not hold a slot against live work. Its symlink stays
        because the other tasks read the same directory, so `staged` cannot
        simply be the symlink count.

        The defect is that the exclusion mixes two units. `_build_staging_context`
        subtracts `failed`, which counts error *files* across tasks, from a
        symlink count that holds each input *file* once.

        Here two files are staged under a cap of 2, and one of them fails in
        `alpha` and `beta` while `gamma` has not processed it yet. The untouched
        file still holds a slot and the failed one frees a single slot however
        many tasks recorded it, so `staged` should be 1, but 2 - 2 reports 0.

        Note that `failed` reading 2 for one bad input is not itself the bug.
        It is documented as a count of error files, so 2 is what it should
        report. Only the subtraction is wrong.

        `test_fan_out_failures_do_not_cut_the_run_short` in test_lifecycle.py
        covers the other symptom of this mismatch.
        """
        pipeline = pipeline_factory(
            [task_spec("alpha"), task_spec("beta"), task_spec("gamma")],
            staging={"steps": [{"kind": "max_staged", "count": 2}]},
        )
        alpha, beta, _gamma = pipeline._config.tasks
        for i in range(6):
            (input_dir / f"f{i}.txt").write_text("x")

        pipeline._stage_new_files()
        staged = [f.name for f in pipeline._symlinks_dir.iterdir()]
        assert len(staged) == 2, "Cap should admit exactly 2 on the first pass"

        failed_name = staged[0]
        failed_stem = Path(failed_name).stem
        for task in (alpha, beta):
            (task.output_dir / f"{failed_stem}.err").write_text("boom")
        pipeline._report_failed_files()

        assert (pipeline._symlinks_dir / failed_name).exists(), (
            "Failure must not remove the symlink the remaining task reads from"
        )

        assert pipeline._build_staging_context().staged == 1, (
            "One of the 2 staged files is untouched and the other can no longer "
            "complete, so one slot is occupied no matter how many tasks recorded "
            "the failure"
        )


class TestFailureReporting:
    """Error files are attributed per task and announced once each."""

    def test_reports_each_error_once(
        self, pipeline_factory: PipelineFactory, error_logs: list[str]
    ):
        """An .err file present across repeated scans is logged only the first time.

        The tracking loop rescans every cycle, so an already-seen file must
        stay silent. Asserting on the log rather than on `_task_error_filenames`
        is deliberate: that is a set, so it holds one entry either way.
        """
        pipeline = pipeline_factory()
        task = pipeline._config.tasks[0]
        (task.output_dir / "a.err").write_text("boom")

        pipeline._report_failed_files()
        pipeline._report_failed_files()

        assert len(error_logs) == 1
        assert task.name in error_logs[0]

    def test_tracks_errors_per_task(self, pipeline_factory: PipelineFactory):
        """Errors are attributed to the task that produced them.

        The tasks fan out from the same input rather than forming a chain, so
        one file can fail in both. `a.err` must then appear under both task
        names, which is what a single shared set would get wrong.
        """
        pipeline = pipeline_factory([task_spec("alpha"), task_spec("beta")])
        alpha, beta = pipeline._config.tasks
        (alpha.output_dir / "a.err").write_text("boom")
        (beta.output_dir / "a.err").write_text("boom")
        (beta.output_dir / "b.err").write_text("boom")

        pipeline._report_failed_files()

        assert pipeline._task_error_filenames["alpha"] == {"a.err"}
        assert pipeline._task_error_filenames["beta"] == {"a.err", "b.err"}
