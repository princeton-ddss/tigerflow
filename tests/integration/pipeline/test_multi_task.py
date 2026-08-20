"""Tests for pipelines with more than one task.

Each task feeds the next along a dependency chain, and a file is completed
only once every terminal task has finished with it.
"""

from pathlib import Path

from .helpers import PipelineFactory, stage_file, task_spec, write_output

CHAIN = [
    task_spec("first"),
    task_spec("second", depends_on="first"),
]

MIXED_EXT_CHAIN = [
    task_spec("first", input_ext=".txt", output_ext=".json"),
    task_spec("second", depends_on="first", input_ext=".json", output_ext=".csv"),
]

FAN_OUT = [
    task_spec("root"),
    task_spec("leafA", depends_on="root"),
    task_spec("leafB", depends_on="root"),
]


class TestTaskWiring:
    """Task directories follow the dependency graph."""

    def test_chain_wires_output_to_next_input(self, pipeline_factory: PipelineFactory):
        """A dependent task reads from its parent's output directory."""
        pipeline = pipeline_factory(CHAIN)
        first, second = pipeline._config.tasks

        assert first.input_dir == pipeline._symlinks_dir
        assert second.input_dir == first.output_dir

    def test_terminal_tasks_identified(self, pipeline_factory: PipelineFactory):
        """Only tasks nothing depends on count as terminal."""
        pipeline = pipeline_factory(FAN_OUT)

        terminal = {t.name for t in pipeline._config.terminal_tasks}
        assert terminal == {"leafA", "leafB"}


class TestChainCompletion:
    """A file is completed only after the final task in a chain finishes."""

    def test_not_finished_until_last_task(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Output from an intermediate task alone does not complete the file."""
        pipeline = pipeline_factory(CHAIN)

        stage_file(pipeline, input_dir, "a")

        write_output(pipeline, "a", task="first")
        pipeline._handle_processed_files()

        assert not (pipeline._finished_dir / "a.txt").exists(), (
            "Only the terminal task's output should complete a file"
        )

        write_output(pipeline, "a", task="second")
        pipeline._handle_processed_files()

        assert (pipeline._finished_dir / "a.txt").exists()

    def test_intermediate_output_cleaned(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Completing a chain removes intermediate outputs as well."""
        pipeline = pipeline_factory(CHAIN)
        first, second = pipeline._config.tasks

        stage_file(pipeline, input_dir, "a")

        write_output(pipeline, "a", task="first")
        pipeline._handle_processed_files()
        write_output(pipeline, "a", task="second")
        pipeline._handle_processed_files()

        assert not (first.output_dir / "a.txt").exists()
        assert not (second.output_dir / "a.txt").exists()


class TestMixedExtensionChain:
    """A chain that changes extension at every step.

    Completion is keyed off the terminal task's `output_ext`, while the
    finished marker and symlink keep the root `input_ext`.
    """

    def test_completes_on_terminal_ext(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Only the terminal `.csv` output completes a `.txt` input."""
        pipeline = pipeline_factory(MIXED_EXT_CHAIN)

        stage_file(pipeline, input_dir, "a")

        write_output(pipeline, "a", task="first")
        pipeline._handle_processed_files()

        assert not (pipeline._finished_dir / "a.txt").exists()

        write_output(pipeline, "a", task="second")
        pipeline._handle_processed_files()

        assert (pipeline._finished_dir / "a.txt").exists()
        assert not (pipeline._symlinks_dir / "a.txt").exists()

    def test_intermediate_outputs_cleaned_at_own_ext(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Each task's output is removed under that task's own extension."""
        pipeline = pipeline_factory(MIXED_EXT_CHAIN)
        first, second = pipeline._config.tasks

        stage_file(pipeline, input_dir, "a")

        write_output(pipeline, "a", task="first")
        pipeline._handle_processed_files()
        write_output(pipeline, "a", task="second")
        pipeline._handle_processed_files()

        assert not (first.output_dir / "a.json").exists()
        assert not (second.output_dir / "a.csv").exists()


class TestFilenameHandling:
    """File IDs are derived by stripping extensions, not by splitting on dots."""

    def test_multi_dot_filename_completes(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """A name containing extra dots survives ID derivation across a chain."""
        pipeline = pipeline_factory(CHAIN)
        filename = "data.v2.txt"

        stage_file(pipeline, input_dir, "data.v2")

        assert (pipeline._symlinks_dir / filename).is_symlink()

        write_output(pipeline, "data.v2", task="first")
        pipeline._handle_processed_files()
        write_output(pipeline, "data.v2", task="second")
        pipeline._handle_processed_files()

        assert (pipeline._finished_dir / filename).exists()
        assert not (pipeline._symlinks_dir / filename).exists()


class TestFanOutCompletion:
    """Every terminal task must finish before a file is completed."""

    def test_one_terminal_task_is_not_enough(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """A file is not completed while a sibling terminal task is outstanding."""
        pipeline = pipeline_factory(FAN_OUT)

        stage_file(pipeline, input_dir, "a")

        write_output(pipeline, "a", task="leafA")
        pipeline._handle_processed_files()

        assert not (pipeline._finished_dir / "a.txt").exists()

    def test_finishes_when_terminal_tasks_agree_in_one_cycle(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Both terminal outputs present in the same cycle complete the file."""
        pipeline = pipeline_factory(FAN_OUT)

        stage_file(pipeline, input_dir, "a")

        write_output(pipeline, "a", task="leafA")
        write_output(pipeline, "a", task="leafB")
        pipeline._handle_processed_files()

        assert (pipeline._finished_dir / "a.txt").exists()
        assert not (pipeline._symlinks_dir / "a.txt").exists()

    def test_finishes_when_terminal_tasks_finish_on_different_cycles(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Terminal tasks finishing at different times must still complete the file.

        Sibling tasks rarely finish within the same polling cycle, so this is
        the ordinary case rather than an edge case.
        """
        pipeline = pipeline_factory(FAN_OUT)

        stage_file(pipeline, input_dir, "a")

        write_output(pipeline, "a", task="leafA")
        pipeline._handle_processed_files()

        write_output(pipeline, "a", task="leafB")
        pipeline._handle_processed_files()

        assert (pipeline._finished_dir / "a.txt").exists(), (
            "File should be completed once every terminal task has finished"
        )

    def test_staggered_completion_does_not_leak(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """A staggered fan-out must not strand its symlink and outputs."""
        pipeline = pipeline_factory(FAN_OUT)

        stage_file(pipeline, input_dir, "a")

        write_output(pipeline, "a", task="leafA")
        pipeline._handle_processed_files()

        write_output(pipeline, "a", task="leafB")
        pipeline._handle_processed_files()

        assert not (pipeline._symlinks_dir / "a.txt").exists()
        for name in ("leafA", "leafB"):
            task = next(t for t in pipeline._config.tasks if t.name == name)
            assert not (task.output_dir / "a.txt").exists()

    def test_completion_stays_recorded_after_output_cleanup(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """A completed file is not reprocessed when its task output reappears.

        Completion cleanup deletes task outputs, but `_task_processed_filenames`
        keeps the record on purpose: pruning it would let a reappearing output
        re-complete the file and copy it out a second time.
        """
        pipeline = pipeline_factory(
            [
                task_spec("root"),
                task_spec("leafA", depends_on="root", keep_output=True),
                task_spec("leafB", depends_on="root", keep_output=True),
            ]
        )

        stage_file(pipeline, input_dir, "a")

        write_output(pipeline, "a", task="leafA")
        pipeline._handle_processed_files()
        write_output(pipeline, "a", task="leafB")
        pipeline._handle_processed_files()

        assert (pipeline._finished_dir / "a.txt").exists()

        kept_copy = pipeline._output_dir / "leafA" / "a.txt"
        assert kept_copy.exists(), "keep_output should have copied the output out"

        # Remove the copy so a repeated copy is detectable rather than silently
        # overwriting the same path
        kept_copy.unlink()

        write_output(pipeline, "a", task="leafA")
        pipeline._handle_processed_files()

        assert not kept_copy.exists(), (
            "A completed file must not be copied out a second time"
        )
        for name in ("leafA", "leafB"):
            assert "a.txt" in pipeline._task_processed_filenames[name], (
                "Completion must stay recorded after the output is cleaned up"
            )
