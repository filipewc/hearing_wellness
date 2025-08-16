from pathlib import Path
import pandas as pd
from src.settings import SILVER_DIR

def main():
    p = SILVER_DIR / "survey_clean.parquet"
    if not p.exists():
        raise FileNotFoundError(p)
    df = pd.read_parquet(p)
    print("\n[inspect] shape:", df.shape)
    print("[inspect] columns:")
    for c in df.columns:
        print(" -", c, "| dtype:", df[c].dtype, "| non-null:", df[c].notna().sum())
    print("\n[inspect] sample rows:")
    with pd.option_context("display.max_columns", None, "display.width", 160):
        print(df.head(5))
    print("\n[inspect] top categories (até 5 por coluna object):")
    for c in df.select_dtypes(include="object"):
        vc = df[c].value_counts(dropna=False).head(5)
        print(f"\n - {c}:\n{vc}")

if __name__ == "__main__":
    main()
