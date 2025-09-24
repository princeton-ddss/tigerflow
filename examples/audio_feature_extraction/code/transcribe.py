from pathlib import Path

import whisper

from tigerflow.tasks import SlurmTask

MODEL_PATH = Path(__file__).parent.parent / "models" / "whisper" / "medium.pt"


class Transcribe(SlurmTask):
    @staticmethod
    def setup(context):
        context.model = whisper.load_model(MODEL_PATH)
        print("Model loaded successfully")

    @staticmethod
    def run(context, input_file, output_file):
        result = context.model.transcribe(str(input_file))
        print(f"Transcription ran successfully for {input_file}")

        with open(output_file, "w") as f:
            f.write(result["text"])


Transcribe.cli()
