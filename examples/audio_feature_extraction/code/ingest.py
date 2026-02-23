import json
from pathlib import Path
from typing import Annotated

import typer

from tigerflow.tasks import LocalTask
from tigerflow.utils import SetupContext

DB_PATH = Path(__file__).parent.parent / "results" / "test.db"


class Ingest(LocalTask):
    class Params:
        db_path: Annotated[
            Path,
            typer.Option(help="Path to the DuckDB database file"),
        ] = DB_PATH

    @staticmethod
    def setup(context: SetupContext):
        import duckdb

        conn = duckdb.connect(str(context.db_path))  # Creates file if not existing
        print(f"Successfully connected to {context.db_path}")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS embeddings (
                id UBIGINT,
                embedding FLOAT[1024],
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        context.conn = conn

    @staticmethod
    def run(context: SetupContext, input_file: Path, output_file: Path):
        with open(input_file) as f:
            content = json.load(f)

        embedding = content["data"][0]["embedding"]

        context.conn.execute(
            "INSERT INTO embeddings (id, embedding) VALUES (?, ?)",
            (input_file.stem, embedding),
        )

    @staticmethod
    def teardown(context: SetupContext):
        context.conn.close()
        print("DB connection closed")


Ingest.cli()
