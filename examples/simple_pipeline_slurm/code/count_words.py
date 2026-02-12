import json
import re
import time
from collections import Counter

from tigerflow.tasks import SlurmTask


class CountUniqueWords(SlurmTask):
    @staticmethod
    def run(context, input_file, output_file):
        with open(input_file) as f:
            content = f.read()

        # Extract and count words made of letters
        words = re.findall(r"\b[a-zA-Z]+\b", content.lower())
        word_counts = Counter(words)
        time.sleep(3)  # Simulate heavy computation

        with open(output_file, "w") as f:
            json.dump(dict(word_counts), f, indent=2)


CountUniqueWords.cli()
