from abc import ABC, abstractmethod
from pathlib import Path


class Task(ABC):
    @classmethod
    @abstractmethod
    def cli(cls):
        """
        Run the task as a CLI application
        """
        pass

    @staticmethod
    def _remove_temporary_files(dirpath: Path):
        """
        Remove any files with no file extension.
        """
        for f in dirpath.iterdir():
            if f.is_file() and f.suffix == "":
                f.unlink()

    @staticmethod
    def _get_unprocessed_files(
        input_dir: Path,
        input_ext: str,
        output_dir: Path,
        output_ext: str,
    ) -> list[Path]:
        """
        Compare input and output directories to identify
        files that have not yet been fully processed.

        Note that the files returned by this function as
        "unprocessed" may include ones still undergoing
        processing. Additional tracking is required to
        exclude such in-progress files.
        """
        processed_ids = {
            f.name.removesuffix(ext)
            for f in output_dir.iterdir()
            for ext in (output_ext, ".err")
            if f.is_file() and f.name.endswith(ext)
        }

        unprocessed_files = [
            f
            for f in input_dir.iterdir()
            if f.is_file()
            and f.name.endswith(input_ext)
            and f.name.removesuffix(input_ext) not in processed_ids
        ]

        return unprocessed_files
