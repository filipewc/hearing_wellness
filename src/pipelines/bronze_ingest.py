from pathlib import Path
import pandas as pd
from datetime import datetime
from src.settings import RAW_DIR, BRONZE_DIR
from src.utils.columns import to_snake, standardize_object_columns

def main():
    csvs = list(RAW_DIR.glob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"Nenhum CSV em {RAW_DIR}")
    raw_file = csvs[0]
    print(f"[bronze] lendo {raw_file.name}")

    # tenta utf-8; se falhar, tenta latin-1
    try:
        df = pd.read_csv(raw_file, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(raw_file, encoding="latin-1")

    # padroniza nomes de colunas
    df.columns = [to_snake(c) for c in df.columns]
    # limpa strings
    df = standardize_object_columns(df)

    # metadados mínimos
    df["_ingestion_ts"] = pd.Timestamp.now(tz="UTC")

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    out = BRONZE_DIR / "survey.parquet"
    df.to_parquet(out, index=False)
    print(f"[bronze] gravado {out}")

if __name__ == "__main__":
    main()
