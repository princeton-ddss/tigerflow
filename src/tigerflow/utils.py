import os
import tempfile
from contextlib import contextmanager


@contextmanager
def atomic_write(
    filepath: os.PathLike,
    mode: str = "w",
    encoding: str | None = None,
    newline: str | None = None,
):
    """
    Atomically write to a file by writing to a temporary file and then replacing the target.
    """
    dir_name = os.path.dirname(filepath) or "."

    # Create temporary file in the same directory
    with tempfile.NamedTemporaryFile(
        mode=mode, encoding=encoding, newline=newline, dir=dir_name, delete=False
    ) as tmp_file:
        temp_path = tmp_file.name
        try:
            yield tmp_file  # Give control to the caller to write into the file
            tmp_file.flush()
            os.fsync(tmp_file.fileno())  # Ensure all data is written to disk
        except Exception:
            os.remove(temp_path)
            raise
        else:
            os.replace(temp_path, filepath)  # Atomic move
