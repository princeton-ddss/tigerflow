from .local import LocalTask
from .local_async import LocalAsyncTask
from .slurm import SlurmTask

__all__ = ["LocalTask", "LocalAsyncTask", "SlurmTask"]
