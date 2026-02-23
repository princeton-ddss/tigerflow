from pathlib import Path
from typing import Annotated

import typer

from tigerflow.tasks import SlurmTask
from tigerflow.utils import SetupContext

MODEL_FILE = Path(__file__).parent.parent / "models" / "whisper" / "medium.pt"


class Transcribe(SlurmTask):
    class Params:
        model_file: Annotated[
            Path,
            typer.Option(help="Path to the Whisper model file"),
        ] = MODEL_FILE

    @staticmethod
    def setup(context: SetupContext):
        import whisper

        context.model = whisper.load_model(str(context.model_file))
        print("Model loaded successfully")

    @staticmethod
    def run(context: SetupContext, input_file: Path, output_file: Path):
        result = context.model.transcribe(str(input_file))
        print(f"Transcription ran successfully for {input_file}")

        with open(output_file, "w") as f:
            f.write(result["text"])


Transcribe.cli()
