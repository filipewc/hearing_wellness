from pathlib import Path
import pandas as pd
from src.settings import BRONZE_DIR, SILVER_DIR

def main():
    src = BRONZE_DIR / "survey.parquet"
    if not src.exists():
        raise FileNotFoundError(f"Arquivo bronze não encontrado: {src}")

    df = pd.read_parquet(src)

    # limpeza genérica: remove duplicidades exatas
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    print(f"[silver] drop_duplicates: {before}->{after}")

    # (opcional) conversions leves — ajuste depois que olharmos o schema real
    # exemplo: normalizar respostas Yes/No se existirem
    for col in df.columns:
        if df[col].dtype == "object":
            uniques = set(map(str.lower, df[col].dropna().astype(str).unique().tolist()))
            if uniques.issubset({"yes", "no", "sim", "não", "nao"}):
                df[col] = df[col].str.lower().map({"yes": True, "no": False, "sim": True, "não": False, "nao": False})

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    out = SILVER_DIR / "survey_clean.parquet"
    df.to_parquet(out, index=False)
    print(f"[silver] gravado {out}")

if __name__ == "__main__":
    main()
