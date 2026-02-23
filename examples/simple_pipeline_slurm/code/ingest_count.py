import json
from pathlib import Path

from tigerflow.tasks import LocalTask
from tigerflow.utils import SetupContext

DB_PATH = Path(__file__).parent.parent / "results" / "test.db"


class Ingest(LocalTask):
    @staticmethod
    def setup(context: SetupContext):
        import sqlite3

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
    def run(context: SetupContext, input_file: Path, output_file: Path):
        with open(input_file) as f:
            content = json.load(f)

        assert isinstance(content, dict)

        context.conn.execute(
            "INSERT INTO books (id, unique_word_count) VALUES (?, ?)",
            (input_file.stem, len(content)),
        )

        context.conn.commit()

    @staticmethod
    def teardown(context: SetupContext):
        context.conn.close()
        print("DB connection closed")


Ingest.cli()
