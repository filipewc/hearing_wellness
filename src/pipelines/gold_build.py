from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from src.logging_conf import setup_logging
from src.settings import SILVER_DIR, GOLD_DIR

logger = logging.getLogger("pipeline")

def main():
    setup_logging()
    src = SILVER_DIR / "survey_clean.parquet"
    if not src.exists():
        raise FileNotFoundError(f"Silver não encontrado: {src}")

    df = pd.read_parquet(src)

    # Faixas etárias somente se age existir
    if "age" in df.columns:
        bins = [0, 18, 30, 45, 60, 200]
        labels = ["0-17", "18-29", "30-44", "45-59", "60+"]
        df["age_band"] = pd.cut(df["age"].astype("float"), bins=bins, labels=labels, right=False)

    # hearing_issues -> boolean, se existir
    if "hearing_issues" in df.columns and df["hearing_issues"].dtype != "bool":
        df["hearing_issues"] = df["hearing_issues"].astype("boolean")

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    # fato (sempre grava)
    fact_out = GOLD_DIR / "fact_survey.parquet"
    fact = df.copy()
    fact["_gold_ts"] = pd.Timestamp.now(tz="UTC")
    fact.to_parquet(fact_out, index=False)

    # KPIs
    kpi = []

    if "hearing_issues" in df.columns:
        prev = df["hearing_issues"].mean(skipna=True) * 100
        kpi.append({"kpi": "prevalence_overall_pct", "value": round(float(prev), 2)})

    if {"hearing_issues","age_band"}.issubset(df.columns):
        by_age = df.groupby("age_band", dropna=False)["hearing_issues"].mean().mul(100).round(2).reset_index()
        by_age.rename(columns={"hearing_issues":"prevalence_pct"}, inplace=True)
        by_age.to_parquet(GOLD_DIR / "kpi_prevalence_by_age.parquet", index=False)

    if {"hearing_issues","gender"}.issubset(df.columns):
        by_gender = df.groupby("gender", dropna=False)["hearing_issues"].mean().mul(100).round(2).reset_index()
        by_gender.rename(columns={"hearing_issues":"prevalence_pct"}, inplace=True)
        by_gender.to_parquet(GOLD_DIR / "kpi_prevalence_by_gender.parquet", index=False)

    if "hearing_aid_use" in df.columns:
        use_rate = df["hearing_aid_use"].mean(skipna=True) * 100
        kpi.append({"kpi": "hearing_aid_use_overall_pct", "value": round(float(use_rate), 2)})

    pd.DataFrame(kpi).to_parquet(GOLD_DIR / "kpi_overview.parquet", index=False)

    logger.info(f"[gold] gerado: {fact_out.name}, kpi_overview.parquet e cortes por age/gender se disponíveis")

if __name__ == "__main__":
    main()
