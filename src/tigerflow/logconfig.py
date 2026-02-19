import sys

from loguru import logger

logger.remove()

# Custom METRICS level for structured timing/performance data
# Severity 25 (between INFO=20 and WARNING=30)
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
