import re
import time
from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

from tigerflow.logconfig import logger
from tigerflow.utils import import_callable, validate_callable_reference, validate_file_ext


@dataclass(frozen=True)
class PipelineState:
    """Read-only view of pipeline state for staging middleware."""

    waiting: int  # Files in input_dir not yet staged
    staged: int  # Files staged but not completed
    completed: int  # Files in .finished directory
    failed: int  # Total error files across tasks
    input_dir: Path  # Reference for companion lookups
    output_dir: Path  # Reference for capacity checks


class BaseStagingMiddleware(BaseModel):
    """Base class for staging middleware."""

    @abstractmethod
    def __call__(self, candidates: list[Path], state: PipelineState) -> list[Path]:
        pass


class MinSizeFilter(BaseStagingMiddleware):
    """Filter files by minimum size."""

    kind: Literal["min_size"]
    bytes: int = Field(gt=0)

    def __call__(self, candidates: list[Path], state: PipelineState) -> list[Path]:
        return [f for f in candidates if f.stat().st_size >= self.bytes]


class MaxSizeFilter(BaseStagingMiddleware):
    """Filter files by maximum size."""

    kind: Literal["max_size"]
    bytes: int = Field(gt=0)

    def __call__(self, candidates: list[Path], state: PipelineState) -> list[Path]:
        return [f for f in candidates if f.stat().st_size <= self.bytes]


class MinAgeFilter(BaseStagingMiddleware):
    """Filter files by minimum age (time since last modification)."""

    kind: Literal["min_age"]
    seconds: float = Field(gt=0)

    def __call__(self, candidates: list[Path], state: PipelineState) -> list[Path]:
        now = time.time()
        return [f for f in candidates if (now - f.stat().st_mtime) >= self.seconds]


class FilenameMatchFilter(BaseStagingMiddleware):
    """Filter files by regex pattern match on filename."""

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

    def __call__(self, candidates: list[Path], state: PipelineState) -> list[Path]:
        regex = re.compile(self.pattern)
        return [f for f in candidates if regex.search(f.name)]


class CompanionFileFilter(BaseStagingMiddleware):
    """Filter files that have a companion file with a specific extension."""

    kind: Literal["companion_file"]
    ext: str

    @field_validator("ext")
    @classmethod
    def validate_ext(cls, ext: str) -> str:
        return validate_file_ext(ext)

    def __call__(self, candidates: list[Path], state: PipelineState) -> list[Path]:
        return [f for f in candidates if f.with_suffix(self.ext).is_file()]


class MaxStagedLimit(BaseStagingMiddleware):
    """Limit total staged files in pipeline."""

    kind: Literal["max_staged"]
    count: int = Field(gt=0)

    def __call__(self, candidates: list[Path], state: PipelineState) -> list[Path]:
        remaining_capacity = max(0, self.count - state.staged)
        return candidates[:remaining_capacity]


class MaxBatchLimit(BaseStagingMiddleware):
    """Limit files staged per polling cycle."""

    kind: Literal["max_batch"]
    count: int = Field(gt=0)

    def __call__(self, candidates: list[Path], state: PipelineState) -> list[Path]:
        return candidates[: self.count]


class SortBy(BaseStagingMiddleware):
    """Sort candidates by attribute for deterministic processing."""

    kind: Literal["sort_by"]
    key: Literal["name", "size", "mtime"] = "name"
    reverse: bool = False

    def __call__(self, candidates: list[Path], state: PipelineState) -> list[Path]:
        if self.key == "name":
            return sorted(candidates, key=lambda f: f.name, reverse=self.reverse)
        elif self.key == "size":
            return sorted(candidates, key=lambda f: f.stat().st_size, reverse=self.reverse)
        elif self.key == "mtime":
            return sorted(candidates, key=lambda f: f.stat().st_mtime, reverse=self.reverse)
        return candidates


def _validate_callable_function(function: str) -> str:
    validate_callable_reference(function)
    try:
        import_callable(function)
    except (ModuleNotFoundError, AttributeError, TypeError) as e:
        raise ValueError(str(e))
    return function


class CallableMiddleware(BaseStagingMiddleware):
    """User-defined middleware via callable reference."""

    kind: Literal["callable"]
    function: str

    @field_validator("function")
    @classmethod
    def validate_function(cls, function: str) -> str:
        return _validate_callable_function(function)

    def __call__(self, candidates: list[Path], state: PipelineState) -> list[Path]:
        fn = import_callable(self.function)
        try:
            return fn(candidates, state)
        except Exception as e:
            logger.warning("Callable '{}' raised: {}", self.function, e)
            return []


StagingMiddlewareConfig = Annotated[
    MinSizeFilter
    | MaxSizeFilter
    | MinAgeFilter
    | FilenameMatchFilter
    | CompanionFileFilter
    | MaxStagedLimit
    | MaxBatchLimit
    | SortBy
    | CallableMiddleware,
    Field(discriminator="kind"),
]


class StagingPipeline(BaseModel):
    """Ordered list of staging middleware."""

    steps: list[StagingMiddlewareConfig] = []

    def process(self, candidates: list[Path], state: PipelineState) -> list[Path]:
        """Run candidates through all middleware steps in order."""
        result = candidates
        for step in self.steps:
            result = step(result, state)
            if not result:
                break
        return result
