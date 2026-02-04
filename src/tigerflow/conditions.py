import re
import time
from abc import abstractmethod
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

from tigerflow.logconfig import logger
from tigerflow.utils import import_callable, validate_callable_reference, validate_file_ext


def _validate_callable_function(function: str) -> str:
    validate_callable_reference(function)
    try:
        import_callable(function)
    except (ModuleNotFoundError, AttributeError, TypeError) as e:
        raise ValueError(str(e))
    return function


class BaseFileCondition(BaseModel):
    @abstractmethod
    def check(self, file: Path) -> bool:
        pass


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


class CallableFileCondition(BaseFileCondition):
    kind: Literal["callable"]
    function: str

    @field_validator("function")
    @classmethod
    def validate_function(cls, function: str) -> str:
        return _validate_callable_function(function)

    def check(self, file: Path) -> bool:
        fn = import_callable(self.function)
        try:
            return fn(file)
        except Exception as e:
            logger.warning("Callable '{}' raised for {}: {}", self.function, file, e)
            return False


FileConditionConfig = Annotated[
    MinSizeCondition
    | MaxSizeCondition
    | MinAgeCondition
    | FilenameMatchCondition
    | CompanionFileCondition
    | CallableFileCondition,
    Field(discriminator="kind"),
]


class CallablePipelineCondition(BaseModel):
    kind: Literal["callable"]
    function: str

    @field_validator("function")
    @classmethod
    def validate_function(cls, function: str) -> str:
        return _validate_callable_function(function)

    def check(self, input_dir: Path) -> bool:
        fn = import_callable(self.function)
        try:
            return fn(input_dir)
        except Exception as e:
            logger.warning("Callable '{}' raised for {}: {}", self.function, input_dir, e)
            return False


PipelineConditionConfig = Annotated[
    CallablePipelineCondition,
    Field(discriminator="kind"),
]


class StagingConditions(BaseModel):
    file: list[FileConditionConfig] = []
    pipeline: list[PipelineConditionConfig] = []

    def check_file(self, file: Path) -> bool:
        return all(condition.check(file) for condition in self.file)

    def check_pipeline(self, input_dir: Path) -> bool:
        return all(condition.check(input_dir) for condition in self.pipeline)
