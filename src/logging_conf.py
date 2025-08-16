from __future__ import annotations

import logging
import logging.config
from pathlib import Path

def setup_logging(log_dir: Path | None = None, level: int = logging.INFO) -> None:
    """
    Configura logging com console + arquivo rotativo (logs/pipeline.log).
    Use no início de cada script:
        from src.logging_conf import setup_logging
        setup_logging()
        logger = logging.getLogger("pipeline")
    """
    if log_dir is None:
        log_dir = Path(__file__).resolve().parents[1] / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "pipeline.log"

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": level,
                "formatter": "simple",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": level,
                "formatter": "simple",
                "filename": str(log_file),
                "maxBytes": 2_000_000,
                "backupCount": 3,
                "encoding": "utf-8",
            },
        },
        "loggers": {
            "pipeline": {
                "handlers": ["console", "file"],
                "level": level,
                "propagate": False,
            }
        },
        "root": {
            "handlers": ["console", "file"],
            "level": level,
        },
    }
    logging.config.dictConfig(config)
