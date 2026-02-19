from pathlib import Path

from tigerflow.tasks import SlurmTask
from tigerflow.utils import SetupContext

MODEL_PATH = Path(__file__).parent.parent / "models" / "whisper" / "medium.pt"


class Transcribe(SlurmTask):
    @staticmethod
    def setup(context: SetupContext):
        import whisper

        context.model = whisper.load_model(MODEL_PATH)
        print("Model loaded successfully")

    @staticmethod
    def run(context: SetupContext, input_file: Path, output_file: Path):
        result = context.model.transcribe(str(input_file))
        print(f"Transcription ran successfully for {input_file}")

        with open(output_file, "w") as f:
            f.write(result["text"])


Transcribe.cli()
