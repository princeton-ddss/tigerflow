import re
import shlex
import subprocess
import time
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

from tigerflow.utils import validate_file_ext


class BaseFileCondition(BaseModel):
    def check(self, file: Path) -> bool:
        raise NotImplementedError


class MinSizeCondition(BaseFileCondition):
    kind: Literal["min_size"]
    bytes: int = Field(gt=0)

    def check(self, file: Path) -> bool:
        return file.stat().st_size >= self.bytes


class MaxSizeCondition(BaseFileCondition):
    kind: Literal["max_size"]
    bytes: int = Field(gt=0)

    def check(self, file: Path) -> bool:
        return file.stat().st_size <= self.bytes


class MinAgeCondition(BaseFileCondition):
    kind: Literal["min_age"]
    seconds: float = Field(gt=0)

    def check(self, file: Path) -> bool:
        return (time.time() - file.stat().st_mtime) >= self.seconds


class FilenameMatchCondition(BaseFileCondition):
    kind: Literal["filename_match"]
    pattern: str

    @field_validator("pattern")
    @classmethod
    def validate_pattern(cls, pattern: str) -> str:
        try:
            re.compile(pattern)
        except re.error as e:
            raise ValueError(f"Invalid regex pattern: {e}")
        return pattern

    def check(self, file: Path) -> bool:
        return re.search(self.pattern, file.name) is not None


class CompanionFileCondition(BaseFileCondition):
    kind: Literal["companion_file"]
    ext: str

    @field_validator("ext")
    @classmethod
    def validate_ext(cls, ext: str) -> str:
        return validate_file_ext(ext)

    def check(self, file: Path) -> bool:
        companion = file.with_suffix(self.ext)
        return companion.is_file()


TaskConditionConfig = Annotated[
    MinSizeCondition
    | MaxSizeCondition
    | MinAgeCondition
    | FilenameMatchCondition
    | CompanionFileCondition,
    Field(discriminator="kind"),
]


class BasePipelineCondition(BaseModel):
    def check(self, input_dir: Path) -> bool:
        raise NotImplementedError


class ScriptCondition(BasePipelineCondition):
    kind: Literal["script"]
    command: str

    def check(self, input_dir: Path) -> bool:
        cmd = f"{self.command} {shlex.quote(str(input_dir))}"
        result = subprocess.run(
            ["bash", "-c", cmd],
            capture_output=True,
        )
        return result.returncode == 0


PipelineConditionConfig = Annotated[
    ScriptCondition,
    Field(discriminator="kind"),
]


class StagingConditions(BaseModel):
    task: list[TaskConditionConfig] = []
    pipeline: list[PipelineConditionConfig] = []

    def check_task(self, file: Path) -> bool:
        return all(condition.check(file) for condition in self.task)

    def check_pipeline(self, input_dir: Path) -> bool:
        return all(condition.check(input_dir) for condition in self.pipeline)
