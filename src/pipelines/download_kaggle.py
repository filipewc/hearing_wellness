from __future__ import annotations

import logging
import os
from pathlib import Path

from src.logging_conf import setup_logging

logger = logging.getLogger("pipeline")

def main():
    setup_logging()
    try:
        import kagglehub  # noqa: F401
    except ModuleNotFoundError as e:
        logger.error("kagglehub não encontrado. Instale com: pip install kagglehub>=0.3.7")
        raise

    dataset = os.environ.get("KAGGLE_DATASET", "adharshinikumar/2025-hearing-wellness-survey")
    logger.info(f"[download] dataset = {dataset}")

    # Baixa e copia para data/raw
    import kagglehub
    cache_path = kagglehub.dataset_download(dataset)
    logger.info(f"[download] cache path: {cache_path}")

    cache = Path(cache_path)
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Dataset tem um único CSV
    candidates = list(cache.glob("*.csv"))
    if not candidates:
        raise FileNotFoundError(f"Nenhum CSV encontrado em {cache}")
    src_file = candidates[0]
    dst_file = raw_dir / src_file.name
    dst_file.write_bytes(src_file.read_bytes())
    logger.info(f"[download] copiado: {src_file.name} -> {dst_file}")
    logger.info("[download] OK")

if __name__ == "__main__":
    main()
