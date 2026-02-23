import json
import re
import time
from collections import Counter
from pathlib import Path

from tigerflow.tasks import LocalTask
from tigerflow.utils import SetupContext


class CountUniqueWords(LocalTask):
    @staticmethod
    def run(context: SetupContext, input_file: Path, output_file: Path):
        with open(input_file) as f:
            content = f.read()

        # Extract and count words made of letters
        words = re.findall(r"\b[a-zA-Z]+\b", content.lower())
        word_counts = Counter(words)
        time.sleep(3)  # Simulate heavy computation

        with open(output_file, "w") as f:
            json.dump(dict(word_counts), f, indent=2)


CountUniqueWords.cli()
