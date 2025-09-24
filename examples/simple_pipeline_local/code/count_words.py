import json
import re
import time
from collections import Counter

from tigerflow.tasks import LocalTask


class CountUniqueWords(LocalTask):
    @staticmethod
    def run(context, input_file, output_file):
        with open(input_file, "r") as fi:
            content = fi.read()

        # Extract and count words made of letters
        words = re.findall(r"\b[a-zA-Z]+\b", content.lower())
        word_counts = Counter(words)
        time.sleep(3)  # Simulate heavy computation

        with open(output_file, "w") as fo:
            json.dump(dict(word_counts), fo, indent=2)


CountUniqueWords.cli()
