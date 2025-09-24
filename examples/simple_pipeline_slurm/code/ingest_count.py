import json
import sqlite3
from pathlib import Path

from tigerflow.tasks import LocalTask

DB_PATH = Path(__file__).parent.parent / "results" / "test.db"


class Ingest(LocalTask):
    @staticmethod
    def setup(context):
        conn = sqlite3.connect(DB_PATH)  # Creates file if not existing
        print(f"Successfully connected to {DB_PATH}")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS books (
                id INTEGER PRIMARY KEY,
                unique_word_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        context.conn = conn

    @staticmethod
    def run(context, input_file, output_file):
        with open(input_file, "r") as f:
            content = json.load(f)

        assert isinstance(content, dict)

        context.conn.execute(
            "INSERT INTO books (id, unique_word_count) VALUES (?, ?)",
            (input_file.stem, len(content)),
        )

        context.conn.commit()

    @staticmethod
    def teardown(context):
        context.conn.close()
        print("DB connection closed")


Ingest.cli()
