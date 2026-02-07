import os
import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from tigerflow.staging import (
    CallableMiddleware,
    CompanionFileFilter,
    FilenameMatchFilter,
    MaxBatchLimit,
    MaxSizeFilter,
    MaxStagedLimit,
    MinAgeFilter,
    MinSizeFilter,
    PipelineState,
    SortBy,
    StagingPipeline,
)


@pytest.fixture
def mock_state(tmp_path: Path) -> PipelineState:
    """Create a mock pipeline state for testing."""
    return PipelineState(
        waiting=10,
        staged=5,
        completed=3,
        failed=1,
        input_dir=tmp_path,
        output_dir=tmp_path / "output",
    )


class TestMinSizeFilter:
    def test_keeps_files_meeting_minimum(self, tmp_path: Path, mock_state: PipelineState):
        file1 = tmp_path / "big.txt"
        file1.write_bytes(b"x" * 100)
        file2 = tmp_path / "small.txt"
        file2.write_bytes(b"x" * 50)
        middleware = MinSizeFilter(kind="min_size", bytes=100)
        result = middleware([file1, file2], mock_state)
        assert result == [file1]

    def test_removes_files_too_small(self, tmp_path: Path, mock_state: PipelineState):
        file = tmp_path / "small.txt"
        file.write_bytes(b"x" * 50)
        middleware = MinSizeFilter(kind="min_size", bytes=100)
        result = middleware([file], mock_state)
        assert result == []

    def test_empty_file_removed(self, tmp_path: Path, mock_state: PipelineState):
        file = tmp_path / "empty.txt"
        file.touch()
        middleware = MinSizeFilter(kind="min_size", bytes=1)
        result = middleware([file], mock_state)
        assert result == []

    def test_rejects_zero_bytes(self):
        with pytest.raises(ValidationError):
            MinSizeFilter(kind="min_size", bytes=0)

    def test_rejects_negative_bytes(self):
        with pytest.raises(ValidationError):
            MinSizeFilter(kind="min_size", bytes=-1)


class TestMaxSizeFilter:
    def test_keeps_files_within_limit(self, tmp_path: Path, mock_state: PipelineState):
        file = tmp_path / "small.txt"
        file.write_bytes(b"x" * 50)
        middleware = MaxSizeFilter(kind="max_size", bytes=100)
        result = middleware([file], mock_state)
        assert result == [file]

    def test_keeps_files_exactly_at_limit(self, tmp_path: Path, mock_state: PipelineState):
        file = tmp_path / "exact.txt"
        file.write_bytes(b"x" * 100)
        middleware = MaxSizeFilter(kind="max_size", bytes=100)
        result = middleware([file], mock_state)
        assert result == [file]

    def test_removes_files_exceeding_limit(self, tmp_path: Path, mock_state: PipelineState):
        file = tmp_path / "big.txt"
        file.write_bytes(b"x" * 200)
        middleware = MaxSizeFilter(kind="max_size", bytes=100)
        result = middleware([file], mock_state)
        assert result == []

    def test_rejects_zero_bytes(self):
        with pytest.raises(ValidationError):
            MaxSizeFilter(kind="max_size", bytes=0)


class TestMinAgeFilter:
    def test_keeps_old_files(self, tmp_path: Path, mock_state: PipelineState):
        file = tmp_path / "old.txt"
        file.touch()
        old_time = time.time() - 10
        os.utime(file, (old_time, old_time))
        middleware = MinAgeFilter(kind="min_age", seconds=5)
        result = middleware([file], mock_state)
        assert result == [file]

    def test_removes_recent_files(self, tmp_path: Path, mock_state: PipelineState):
        file = tmp_path / "new.txt"
        file.touch()
        middleware = MinAgeFilter(kind="min_age", seconds=9999)
        result = middleware([file], mock_state)
        assert result == []

    def test_rejects_zero_seconds(self):
        with pytest.raises(ValidationError):
            MinAgeFilter(kind="min_age", seconds=0)

    def test_rejects_negative_seconds(self):
        with pytest.raises(ValidationError):
            MinAgeFilter(kind="min_age", seconds=-1)


class TestFilenameMatchFilter:
    def test_keeps_matching_files(self, tmp_path: Path, mock_state: PipelineState):
        file = tmp_path / "recording_001.mp3"
        file.touch()
        middleware = FilenameMatchFilter(kind="filename_match", pattern=r"recording_\d+")
        result = middleware([file], mock_state)
        assert result == [file]

    def test_removes_non_matching_files(self, tmp_path: Path, mock_state: PipelineState):
        file = tmp_path / "notes.txt"
        file.touch()
        middleware = FilenameMatchFilter(kind="filename_match", pattern=r"recording_\d+")
        result = middleware([file], mock_state)
        assert result == []

    def test_partial_match_kept(self, tmp_path: Path, mock_state: PipelineState):
        file = tmp_path / "my_recording_001_final.mp3"
        file.touch()
        middleware = FilenameMatchFilter(kind="filename_match", pattern=r"recording_\d+")
        result = middleware([file], mock_state)
        assert result == [file]

    def test_rejects_invalid_regex(self):
        with pytest.raises(ValidationError, match="Invalid regex pattern"):
            FilenameMatchFilter(kind="filename_match", pattern=r"[invalid")


class TestCompanionFileFilter:
    def test_keeps_files_with_companion(self, tmp_path: Path, mock_state: PipelineState):
        file = tmp_path / "data.mp3"
        file.touch()
        companion = tmp_path / "data.done"
        companion.touch()
        middleware = CompanionFileFilter(kind="companion_file", ext=".done")
        result = middleware([file], mock_state)
        assert result == [file]

    def test_removes_files_without_companion(self, tmp_path: Path, mock_state: PipelineState):
        file = tmp_path / "data.mp3"
        file.touch()
        middleware = CompanionFileFilter(kind="companion_file", ext=".done")
        result = middleware([file], mock_state)
        assert result == []

    def test_rejects_invalid_ext(self):
        with pytest.raises(ValidationError):
            CompanionFileFilter(kind="companion_file", ext="no_dot")

    def test_rejects_reserved_err_ext(self):
        with pytest.raises(ValidationError, match="reserved"):
            CompanionFileFilter(kind="companion_file", ext=".err")


class TestMaxStagedLimit:
    def test_limits_based_on_staged_count(self, tmp_path: Path):
        files = [tmp_path / f"file{i}.txt" for i in range(10)]
        for f in files:
            f.touch()
        state = PipelineState(
            waiting=10,
            staged=8,
            completed=0,
            failed=0,
            input_dir=tmp_path,
            output_dir=tmp_path / "output",
        )
        middleware = MaxStagedLimit(kind="max_staged", count=10)
        result = middleware(files, state)
        assert len(result) == 2  # Only 2 more can be staged

    def test_returns_empty_when_at_capacity(self, tmp_path: Path):
        files = [tmp_path / f"file{i}.txt" for i in range(5)]
        for f in files:
            f.touch()
        state = PipelineState(
            waiting=5,
            staged=10,
            completed=0,
            failed=0,
            input_dir=tmp_path,
            output_dir=tmp_path / "output",
        )
        middleware = MaxStagedLimit(kind="max_staged", count=10)
        result = middleware(files, state)
        assert result == []

    def test_rejects_zero_count(self):
        with pytest.raises(ValidationError):
            MaxStagedLimit(kind="max_staged", count=0)


class TestMaxBatchLimit:
    def test_limits_files_per_cycle(self, tmp_path: Path, mock_state: PipelineState):
        files = [tmp_path / f"file{i}.txt" for i in range(10)]
        for f in files:
            f.touch()
        middleware = MaxBatchLimit(kind="max_batch", count=3)
        result = middleware(files, mock_state)
        assert len(result) == 3
        assert result == files[:3]

    def test_returns_all_if_under_limit(self, tmp_path: Path, mock_state: PipelineState):
        files = [tmp_path / f"file{i}.txt" for i in range(2)]
        for f in files:
            f.touch()
        middleware = MaxBatchLimit(kind="max_batch", count=10)
        result = middleware(files, mock_state)
        assert result == files

    def test_rejects_zero_count(self):
        with pytest.raises(ValidationError):
            MaxBatchLimit(kind="max_batch", count=0)


class TestSortBy:
    def test_sorts_by_name(self, tmp_path: Path, mock_state: PipelineState):
        file_c = tmp_path / "c.txt"
        file_a = tmp_path / "a.txt"
        file_b = tmp_path / "b.txt"
        for f in [file_c, file_a, file_b]:
            f.touch()
        middleware = SortBy(kind="sort_by", key="name")
        result = middleware([file_c, file_a, file_b], mock_state)
        assert result == [file_a, file_b, file_c]

    def test_sorts_by_name_reverse(self, tmp_path: Path, mock_state: PipelineState):
        file_a = tmp_path / "a.txt"
        file_b = tmp_path / "b.txt"
        for f in [file_a, file_b]:
            f.touch()
        middleware = SortBy(kind="sort_by", key="name", reverse=True)
        result = middleware([file_a, file_b], mock_state)
        assert result == [file_b, file_a]

    def test_sorts_by_size(self, tmp_path: Path, mock_state: PipelineState):
        file_big = tmp_path / "big.txt"
        file_big.write_bytes(b"x" * 100)
        file_small = tmp_path / "small.txt"
        file_small.write_bytes(b"x" * 10)
        middleware = SortBy(kind="sort_by", key="size")
        result = middleware([file_big, file_small], mock_state)
        assert result == [file_small, file_big]

    def test_sorts_by_mtime(self, tmp_path: Path, mock_state: PipelineState):
        file_old = tmp_path / "old.txt"
        file_old.touch()
        old_time = time.time() - 100
        os.utime(file_old, (old_time, old_time))
        file_new = tmp_path / "new.txt"
        file_new.touch()
        middleware = SortBy(kind="sort_by", key="mtime")
        result = middleware([file_new, file_old], mock_state)
        assert result == [file_old, file_new]


class TestCallableMiddleware:
    def test_calls_function_with_candidates_and_state(
        self, tmp_path: Path, mock_state: PipelineState
    ):
        # Create a test module with a staging function
        test_module = tmp_path / "test_staging_func.py"
        test_module.write_text(
            """
def keep_first(candidates, state):
    return candidates[:1]
"""
        )
        import sys

        sys.path.insert(0, str(tmp_path))
        try:
            files = [tmp_path / f"file{i}.txt" for i in range(3)]
            for f in files:
                f.touch()
            middleware = CallableMiddleware(
                kind="callable", function="test_staging_func:keep_first"
            )
            result = middleware(files, mock_state)
            assert result == [files[0]]
        finally:
            sys.path.remove(str(tmp_path))

    def test_returns_empty_on_exception(self, tmp_path: Path, mock_state: PipelineState):
        test_module = tmp_path / "test_error_func.py"
        test_module.write_text(
            """
def raise_error(candidates, state):
    raise ValueError("test error")
"""
        )
        import sys

        sys.path.insert(0, str(tmp_path))
        try:
            middleware = CallableMiddleware(
                kind="callable", function="test_error_func:raise_error"
            )
            result = middleware([tmp_path / "file.txt"], mock_state)
            assert result == []
        finally:
            sys.path.remove(str(tmp_path))

    def test_rejects_invalid_reference(self):
        with pytest.raises(ValidationError):
            CallableMiddleware(kind="callable", function="not_a_module")

    def test_rejects_non_importable_function(self):
        with pytest.raises(ValidationError):
            CallableMiddleware(kind="callable", function="nonexistent_module_xyz:func")


class TestStagingPipeline:
    def test_defaults_to_empty_steps(self):
        pipeline = StagingPipeline()
        assert pipeline.steps == []

    def test_process_passes_all_with_no_steps(
        self, tmp_path: Path, mock_state: PipelineState
    ):
        files = [tmp_path / "a.txt", tmp_path / "b.txt"]
        for f in files:
            f.touch()
        pipeline = StagingPipeline()
        result = pipeline.process(files, mock_state)
        assert result == files

    def test_process_chains_middleware(self, tmp_path: Path, mock_state: PipelineState):
        file_big = tmp_path / "big.txt"
        file_big.write_bytes(b"x" * 100)
        file_small = tmp_path / "small.txt"
        file_small.write_bytes(b"x" * 10)
        pipeline = StagingPipeline(
            steps=[
                MinSizeFilter(kind="min_size", bytes=50),
                MaxBatchLimit(kind="max_batch", count=1),
            ]
        )
        result = pipeline.process([file_big, file_small], mock_state)
        assert result == [file_big]

    def test_process_short_circuits_on_empty(
        self, tmp_path: Path, mock_state: PipelineState
    ):
        file = tmp_path / "small.txt"
        file.write_bytes(b"x" * 10)
        pipeline = StagingPipeline(
            steps=[
                MinSizeFilter(kind="min_size", bytes=1000),  # This filters everything
                MaxBatchLimit(kind="max_batch", count=1),  # This should not run
            ]
        )
        result = pipeline.process([file], mock_state)
        assert result == []

    def test_from_yaml_dict(self):
        data = {
            "steps": [
                {"kind": "min_size", "bytes": 1024},
                {"kind": "filename_match", "pattern": r"^recording"},
                {"kind": "max_batch", "count": 10},
            ]
        }
        pipeline = StagingPipeline.model_validate(data)
        assert len(pipeline.steps) == 3
