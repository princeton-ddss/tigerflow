import time
from abc import ABC, abstractmethod
from pathlib import Path

from dask.distributed import Client, Future, Worker, WorkerPlugin, get_worker
from dask_jobqueue import SLURMCluster

from .config import SlurmResourceConfig
from .utils import atomic_write


class SlurmTask(ABC):
    """
    Execute the user-defined task in parallel by distributing
    the workload across Slurm jobs acting as cluster workers.
    """

    def __init__(
        self,
        resources: SlurmResourceConfig,
        setup_commands: str | None = None,
    ):
        self.resources = resources
        self.setup_commands = setup_commands

    def start(self, input_dir: Path, output_dir: Path):
        if not input_dir.exists():
            raise FileNotFoundError(f"Input directory {input_dir} does not exist")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Reference methods that must be implemented in subclass
        setup_func = type(self).setup
        run_func = type(self).run

        class TaskWorkerPlugin(WorkerPlugin):
            def setup(self, worker: Worker):
                setup_func(worker)

        def task(input_file: Path, output_file: Path):
            worker = get_worker()
            try:
                with atomic_write(output_file) as temp_file:
                    run_func(worker, input_file, temp_file)
            except Exception as e:
                with atomic_write(output_file.with_suffix(".err")) as temp_file:
                    with open(temp_file, "w") as f:
                        f.write(e)

        # Define parameters for each Slurm job
        cluster = SLURMCluster(
            cores=self.resources.cpus,
            memory=self.resources.memory,
            walltime=self.resources.time,
            processes=1,
            worker_extra_args=(
                [f"--gres=gpu:{self.resources.gpus}"] if self.resources.gpus else None
            ),
            job_script_prologue=(
                self.setup_commands.strip().split("\n") if self.setup_commands else None
            ),
            local_directory=output_dir,
            log_directory=output_dir,
        )

        # Enable autoscaling
        cluster.adapt(
            minimum_jobs=0,
            maximum_jobs=self.resources.max_workers,
        )

        # Instantiate a cluster client
        client = Client(cluster)
        client.register_plugin(TaskWorkerPlugin())

        # Poll for new files to process
        active_futures: dict[str, Future] = dict()
        while True:
            processed_ids = {
                f.stem
                for f in output_dir.iterdir()
                if f.is_file() and f.suffix in {".out", ".err"}
            }

            unprocessed_files = [
                f
                for f in input_dir.iterdir()
                if f.is_file() and f.suffix == ".out" and f.stem not in processed_ids
            ]

            for file in unprocessed_files:
                if file.stem not in active_futures:  # Prevent duplicate processing
                    output_file = output_dir / file.with_suffix(".out").name
                    future = client.submit(task, file, output_file)
                    active_futures[file.stem] = future

            for key in list(active_futures.keys()):
                if active_futures[key].done():
                    del active_futures[key]

            time.sleep(3)

    @staticmethod
    @abstractmethod
    def setup(worker: Worker):
        """
        Establish a shared processing setup (e.g., model loading).
        """
        pass

    @staticmethod
    @abstractmethod
    def run(worker: Worker, input_file: Path, output_file: Path):
        """
        Specify the processing logic to be applied to each input file.
        """
        pass
