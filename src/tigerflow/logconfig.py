import sys

from loguru import logger

logger.remove()

logger.level("INIT", no=25, color="<blue>", icon="🚀")
logger.level("METRICS", no=25, color="<green>", icon="📊")

logger.add(
    sys.stderr,
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<level>{message}</level>"
    ),
    level="INFO",
)
