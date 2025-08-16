from pathlib import Path
import pandas as pd
from src.settings import BRONZE_DIR, SILVER_DIR
from src.utils.schema_map import build_mapping

YESNO_TRUE = {"yes","sim","true","1","y","t"}
YESNO_FALSE = {"no","não","nao","false","0","n","f"}

def to_bool_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str).str.strip().str.lower()
        .map(lambda x: True if x in YESNO_TRUE else (False if x in YESNO_FALSE else pd.NA))
    )

def main():
    src = BRONZE_DIR / "survey.parquet"
    if not src.exists():
        raise FileNotFoundError(f"Arquivo bronze não encontrado: {src}")

    df = pd.read_parquet(src)

    # limpeza genérica
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    print(f"[silver] drop_duplicates: {before}->{after}")

    # ===== CANONIZAÇÃO DE CAMPOS =====
    mapping = build_mapping(df.columns.tolist())
    print(f"[silver] mapping: {mapping}")

    # AGE
    age_col = mapping.get("age")
    if age_col and age_col not in df.columns:
        age_col = None
    if age_col:
        # tenta converter idade para numérico
        df["age"] = pd.to_numeric(df[age_col], errors="coerce")
        # valores claramente impossíveis -> NaN
        df.loc[(df["age"] < 0) | (df["age"] > 120), "age"] = pd.NA

    # GENDER
    gender_col = mapping.get("gender")
    if gender_col and gender_col in df.columns:
        g = df[gender_col].astype(str).str.strip().str.lower()
        df["gender"] = (
            g.replace({
                "masculino":"male","feminino":"female",
                "m":"male","f":"female",
            })
        )

    # HEARING_ISSUES (bool)
    hi_col = mapping.get("hearing_issues")
    if hi_col and hi_col in df.columns:
        df["hearing_issues"] = to_bool_series(df[hi_col])

    # HEARING_AID_USE (bool) - opcional
    hau_col = mapping.get("hearing_aid_use")
    if hau_col and hau_col in df.columns:
        df["hearing_aid_use"] = to_bool_series(df[hau_col])

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    out = SILVER_DIR / "survey_clean.parquet"
    df.to_parquet(out, index=False)
    print(f"[silver] gravado {out}")

if __name__ == "__main__":
    main()
