import inspect
import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import get_type_hints


class Task(ABC):
    @classmethod
    @abstractmethod
    def cli(cls):
        """
        Run the task as a CLI application
        """
        pass

    @classmethod
    def build_cli(cls, base_main):
        """
        Wrap a base CLI main function to include custom Params as CLI options.

        Inspects cls.Params for additional parameters and creates a new function
        with a combined signature that Typer can use. Also filters out internal
        parameters (starting with _) that Typer cannot handle.
        """
        # Get the base function's signature, excluding internal params (like _params)
        base_sig = inspect.signature(base_main)
        base_params = [
            p for p in base_sig.parameters.values() if not p.name.startswith("_")
        ]

        params_spec = cls._get_params_from_class()
        if not params_spec:
            # No custom params, but still need to filter out internal params
            new_sig = base_sig.replace(parameters=base_params)

            def wrapper(*args, **kwargs):
                return base_main(*args, _params={}, **kwargs)

            wrapper.__signature__ = new_sig
            wrapper.__doc__ = base_main.__doc__
            return wrapper

        # Build new parameters from Params class
        custom_params = []
        for name, (type_hint, default) in params_spec.items():
            param = inspect.Parameter(
                name,
                inspect.Parameter.KEYWORD_ONLY,
                default=default,
                annotation=type_hint,
            )
            custom_params.append(param)

        # Check for name collisions between base and custom params
        base_names = {p.name for p in base_params}
        custom_names = {p.name for p in custom_params}
        collisions = base_names & custom_names
        if collisions:
            raise ValueError(
                f"Parameter name collision in {cls.__name__}.Params: {collisions}. "
                f"These names are reserved: {base_names}"
            )

        # Combine base params with custom params
        new_params = base_params + custom_params
        new_sig = base_sig.replace(parameters=new_params)

        # Create wrapper that separates custom params
        custom_keys = set(params_spec.keys())

        def wrapper(*args, **kwargs):
            custom_values = {k: kwargs.pop(k) for k in list(kwargs) if k in custom_keys}
            return base_main(*args, _params=custom_values, **kwargs)

        wrapper.__signature__ = new_sig
        wrapper.__doc__ = base_main.__doc__

        return wrapper

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

    @classmethod
    def _get_params_from_class(cls) -> dict:
        """
        Extract parameter definitions from the task's Params inner class.

        Returns a dict of {name: (type_hint, default)} where default is
        inspect.Parameter.empty if no default is provided.
        """
        if not hasattr(cls, "Params"):
            return {}

        params = {}
        hints = get_type_hints(cls.Params, include_extras=True)

        for name, type_hint in hints.items():
            if hasattr(cls.Params, name):
                default = getattr(cls.Params, name)
            else:
                default = inspect.Parameter.empty
            params[name] = (type_hint, default)

        return params
