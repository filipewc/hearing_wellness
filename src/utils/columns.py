import re
import pandas as pd

def to_snake(name: str) -> str:
    s = re.sub(r'[^0-9A-Za-z]+', '_', name).strip('_')
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    return s.lower()

def standardize_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    obj_cols = df.select_dtypes(include='object').columns
    for c in obj_cols:
        df[c] = df[c].astype(str).str.strip()
    return df
