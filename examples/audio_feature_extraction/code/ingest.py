import json
from pathlib import Path

import duckdb

from tigerflow.tasks import LocalTask

DB_PATH = Path(__file__).parent.parent / "results" / "test.db"


class Ingest(LocalTask):
    @staticmethod
    def setup(context):
        conn = duckdb.connect(DB_PATH)  # Creates file if not existing
        print(f"Successfully connected to {DB_PATH}")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS embeddings (
                id UBIGINT,
                embedding FLOAT[1024],
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        context.conn = conn

    @staticmethod
    def run(context, input_file, output_file):
        with open(input_file, "r") as f:
            content = json.load(f)

        embedding = content["data"][0]["embedding"]

        context.conn.execute(
            "INSERT INTO embeddings (id, embedding) VALUES (?, ?)",
            (input_file.stem, embedding),
        )

    @staticmethod
    def teardown(context):
        context.conn.close()
        print("DB connection closed")


Ingest.cli()
