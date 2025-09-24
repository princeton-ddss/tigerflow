from pathlib import Path

import whisper

MODEL_PATH = Path(__file__).parent / "medium.pt"

if MODEL_PATH.exists():
    print("Model already downloaded at:", MODEL_PATH)
else:
    whisper.load_model("medium", download_root=MODEL_PATH.parent)
