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

    # ===== CANON
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

    # Heurística: tentar achar coluna-idade se mapping falhar
    def guess_age_col(df):
        num_cols = df.select_dtypes(include=["int64","float64","Int64","Float64"]).columns
        best = None; best_share = 0
        for c in num_cols:
            s = pd.to_numeric(df[c], errors="coerce")
            ok = s.between(0, 120).mean()
            if ok > 0.7 and ok > best_share:
                best, best_share = c, ok
        return best

    age_col = mapping.get("age") or guess_age_col(df)
    if age_col and age_col in df.columns:
        df["age"] = pd.to_numeric(df[age_col], errors="coerce")
        df.loc[(df["age"] < 0) | (df["age"] > 120), "age"] = pd.NA

    gender_col = mapping.get("gender")
    if gender_col and gender_col in df.columns:
        g = df[gender_col].astype(str).str.strip().str.lower()
        df["gender"] = g.replace({"masculino":"male","feminino":"female","m":"male","f":"female"})

    hi_col = mapping.get("hearing_issues")
    if hi_col and hi_col in df.columns:
        df["hearing_issues"] = to_bool_series(df[hi_col])

    hau_col = mapping.get("hearing_aid_use")
    if hau_col and hau_col in df.columns:
        df["hearing_aid_use"] = to_bool_series(df[hau_col])

    # salvar
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    out = SILVER_DIR / "survey_clean.parquet"
    df.to_parquet(out, index=False)
    print(f"[silver] gravado {out}")

if __name__ == "__main__":
    main()
