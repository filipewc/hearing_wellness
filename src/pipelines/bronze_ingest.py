from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.logging_conf import setup_logging
from src.settings import RAW_DIR, BRONZE_DIR
from src.utils.columns import to_snake, standardize_object_columns

logger = logging.getLogger("pipeline")

def main():
    setup_logging()
    csvs = list(RAW_DIR.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"Nenhum CSV em {RAW_DIR}")
    raw_file = csvs[0]
    logger.info(f"[bronze] lendo {raw_file.name}")

    # tenta utf-8; se falhar, tenta latin-1
    try:
        df = pd.read_csv(raw_file, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(raw_file, encoding="latin-1")

    # padroniza nomes de colunas + limpeza de strings
    df.columns = [to_snake(c) for c in df.columns]
    df = standardize_object_columns(df)

    # metadados
    df["_ingestion_ts"] = pd.Timestamp.now(tz="UTC")

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    out = BRONZE_DIR / "survey.parquet"
    df.to_parquet(out, index=False)
    logger.info(f"[bronze] gravado {out}")

if __name__ == "__main__":
    main()
