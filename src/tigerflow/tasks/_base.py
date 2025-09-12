import sys
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

    @classmethod
    def get_name(cls) -> str:
        return cls.__name__

    @classmethod
    def get_module_path(cls) -> Path:
        """
        Return the absolute path to the module file
        where the class is defined.
        """
        module = sys.modules.get(cls.__module__)
        if module is None or not hasattr(module, "__file__"):
            raise FileNotFoundError(f"Module not found for {cls}")

        return Path(module.__file__).resolve()

    @staticmethod
    def _remove_temporary_files(dirpath: Path):
        """
        Remove any files with no file extension.
        """
        for file in dirpath.iterdir():
            if file.is_file() and file.suffix == "":
                file.unlink()

    @staticmethod
    def _get_unprocessed_files(
        *,
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
            file.name.removesuffix(ext)
            for file in output_dir.iterdir()
            for ext in (output_ext, ".err")
            if file.is_file() and file.name.endswith(ext)
        }

        unprocessed_files = [
            file
            for file in input_dir.iterdir()
            if file.is_file()
            and file.name.endswith(input_ext)
            and file.name.removesuffix(input_ext) not in processed_ids
        ]

        return unprocessed_files
