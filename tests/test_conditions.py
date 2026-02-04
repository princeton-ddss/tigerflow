import os
import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from tigerflow.conditions import (
    CallableFileCondition,
    CallableDirectoryCondition,
    CompanionFileCondition,
    FilenameMatchCondition,
    MaxSizeCondition,
    MinAgeCondition,
    MinSizeCondition,
    StagingConditions,
)


class TestMinSizeCondition:
    def test_passes_when_file_meets_minimum(self, tmp_path: Path):
        file = tmp_path / "data.txt"
        file.write_bytes(b"x" * 100)
        condition = MinSizeCondition(kind="min_size", bytes=100)
        assert condition.check(file) is True

    def test_fails_when_file_too_small(self, tmp_path: Path):
        file = tmp_path / "data.txt"
        file.write_bytes(b"x" * 50)
        condition = MinSizeCondition(kind="min_size", bytes=100)
        assert condition.check(file) is False

    def test_fails_on_empty_file(self, tmp_path: Path):
        file = tmp_path / "empty.txt"
        file.touch()
        condition = MinSizeCondition(kind="min_size", bytes=1)
        assert condition.check(file) is False

    def test_rejects_zero_bytes(self):
        with pytest.raises(ValidationError):
            MinSizeCondition(kind="min_size", bytes=0)

    def test_rejects_negative_bytes(self):
        with pytest.raises(ValidationError):
            MinSizeCondition(kind="min_size", bytes=-1)


class TestMaxSizeCondition:
    def test_passes_when_file_within_limit(self, tmp_path: Path):
        file = tmp_path / "data.txt"
        file.write_bytes(b"x" * 50)
        condition = MaxSizeCondition(kind="max_size", bytes=100)
        assert condition.check(file) is True

    def test_passes_when_file_exactly_at_limit(self, tmp_path: Path):
        file = tmp_path / "data.txt"
        file.write_bytes(b"x" * 100)
        condition = MaxSizeCondition(kind="max_size", bytes=100)
        assert condition.check(file) is True

    def test_fails_when_file_exceeds_limit(self, tmp_path: Path):
        file = tmp_path / "data.txt"
        file.write_bytes(b"x" * 200)
        condition = MaxSizeCondition(kind="max_size", bytes=100)
        assert condition.check(file) is False

    def test_rejects_zero_bytes(self):
        with pytest.raises(ValidationError):
            MaxSizeCondition(kind="max_size", bytes=0)


class TestMinAgeCondition:
    def test_passes_when_file_old_enough(self, tmp_path: Path):
        file = tmp_path / "data.txt"
        file.touch()
        # Backdate the file's mtime by 10 seconds
        old_time = time.time() - 10
        os.utime(file, (old_time, old_time))
        condition = MinAgeCondition(kind="min_age", seconds=5)
        assert condition.check(file) is True

    def test_fails_when_file_too_recent(self, tmp_path: Path):
        file = tmp_path / "data.txt"
        file.touch()
        condition = MinAgeCondition(kind="min_age", seconds=9999)
        assert condition.check(file) is False

    def test_rejects_zero_seconds(self):
        with pytest.raises(ValidationError):
            MinAgeCondition(kind="min_age", seconds=0)

    def test_rejects_negative_seconds(self):
        with pytest.raises(ValidationError):
            MinAgeCondition(kind="min_age", seconds=-1)


class TestFilenameMatchCondition:
    def test_passes_on_match(self, tmp_path: Path):
        file = tmp_path / "recording_001.mp3"
        file.touch()
        condition = FilenameMatchCondition(kind="filename_match", pattern=r"recording_\d+")
        assert condition.check(file) is True

    def test_fails_on_no_match(self, tmp_path: Path):
        file = tmp_path / "notes.txt"
        file.touch()
        condition = FilenameMatchCondition(kind="filename_match", pattern=r"recording_\d+")
        assert condition.check(file) is False

    def test_partial_match_passes(self, tmp_path: Path):
        file = tmp_path / "my_recording_001_final.mp3"
        file.touch()
        condition = FilenameMatchCondition(kind="filename_match", pattern=r"recording_\d+")
        assert condition.check(file) is True

    def test_rejects_invalid_regex(self):
        with pytest.raises(ValidationError, match="Invalid regex pattern"):
            FilenameMatchCondition(kind="filename_match", pattern=r"[invalid")


class TestCompanionFileCondition:
    def test_passes_when_companion_exists(self, tmp_path: Path):
        file = tmp_path / "data.mp3"
        file.touch()
        companion = tmp_path / "data.done"
        companion.touch()
        condition = CompanionFileCondition(kind="companion_file", ext=".done")
        assert condition.check(file) is True

    def test_fails_when_companion_missing(self, tmp_path: Path):
        file = tmp_path / "data.mp3"
        file.touch()
        condition = CompanionFileCondition(kind="companion_file", ext=".done")
        assert condition.check(file) is False

    def test_rejects_invalid_ext(self):
        with pytest.raises(ValidationError):
            CompanionFileCondition(kind="companion_file", ext="no_dot")

    def test_rejects_reserved_err_ext(self):
        with pytest.raises(ValidationError, match="reserved"):
            CompanionFileCondition(kind="companion_file", ext=".err")


class TestCallableFileCondition:
    def test_passes_when_callable_returns_true(self, tmp_path: Path):
        file = tmp_path / "data.txt"
        file.touch()
        condition = CallableFileCondition(kind="callable", function="os.path:exists")
        assert condition.check(file) is True

    def test_fails_when_callable_returns_false(self, tmp_path: Path):
        file = tmp_path / "nonexistent.txt"
        condition = CallableFileCondition(kind="callable", function="os.path:exists")
        assert condition.check(file) is False

    def test_rejects_invalid_reference(self):
        with pytest.raises(ValidationError):
            CallableFileCondition(kind="callable", function="not_a_module")

    def test_rejects_non_importable_function(self):
        with pytest.raises(ValidationError):
            CallableFileCondition(kind="callable", function="nonexistent_module_xyz:func")


class TestCallableDirectoryCondition:
    def test_passes_when_callable_returns_true(self, tmp_path: Path):
        condition = CallableDirectoryCondition(kind="callable", function="os.path:isdir")
        assert condition.check(tmp_path) is True

    def test_fails_when_callable_returns_false(self, tmp_path: Path):
        condition = CallableDirectoryCondition(kind="callable", function="os.path:isfile")
        assert condition.check(tmp_path) is False

    def test_rejects_invalid_reference(self):
        with pytest.raises(ValidationError):
            CallableDirectoryCondition(kind="callable", function="bad")

    def test_rejects_non_importable_function(self):
        with pytest.raises(ValidationError):
            CallableDirectoryCondition(kind="callable", function="nonexistent_module_xyz:func")


class TestStagingConditions:
    def test_defaults_to_empty(self):
        conditions = StagingConditions()
        assert conditions.file == []
        assert conditions.directory == []

    def test_check_file_passes_with_no_conditions(self, tmp_path: Path):
        file = tmp_path / "data.txt"
        file.touch()
        conditions = StagingConditions()
        assert conditions.check_file(file) is True

    def test_check_directory_passes_with_no_conditions(self, tmp_path: Path):
        conditions = StagingConditions()
        assert conditions.check_directory(tmp_path) is True

    def test_check_file_all_must_pass(self, tmp_path: Path):
        file = tmp_path / "data.txt"
        file.write_bytes(b"x" * 50)
        conditions = StagingConditions(
            file=[
                MinSizeCondition(kind="min_size", bytes=10),
                MaxSizeCondition(kind="max_size", bytes=100),
            ]
        )
        assert conditions.check_file(file) is True

    def test_check_file_fails_if_any_fails(self, tmp_path: Path):
        file = tmp_path / "data.txt"
        file.write_bytes(b"x" * 200)
        conditions = StagingConditions(
            file=[
                MinSizeCondition(kind="min_size", bytes=10),
                MaxSizeCondition(kind="max_size", bytes=100),
            ]
        )
        assert conditions.check_file(file) is False

    def test_check_directory_fails_if_callable_fails(self, tmp_path: Path):
        file = tmp_path / "not_a_dir.txt"
        file.touch()
        conditions = StagingConditions(
            directory=[CallableDirectoryCondition(kind="callable", function="os.path:isdir")]
        )
        assert conditions.check_directory(file) is False

    def test_from_yaml_dict(self):
        data = {
            "file": [
                {"kind": "min_size", "bytes": 1024},
                {"kind": "filename_match", "pattern": r"^recording"},
            ],
            "directory": [
                {"kind": "callable", "function": "os.path:isdir"},
            ],
        }
        conditions = StagingConditions.model_validate(data)
        assert len(conditions.file) == 2
        assert len(conditions.directory) == 1
