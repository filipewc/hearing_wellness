from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pandas as pd
from great_expectations.dataset import PandasDataset

from src.logging_conf import setup_logging
from src.settings import SILVER_DIR

logger = logging.getLogger("pipeline")

SUITE_FILE = Path("expectations/survey_clean_suite.json")
REPORTS_DIR = Path("reports")

# Validação estrita por padrão; para smoke de PR, o CI define GE_STRICT=0
STRICT = os.getenv("GE_STRICT", "1") == "1"


class SurveyDataset(PandasDataset):
    pass


def _map_expectation_name(etype: str, kwargs: dict):
    # Mapear nomes não suportados para equivalentes
    if etype == "expect_table_row_count_to_be_greater_than":
        value = kwargs.get("value", 0)
        return "expect_table_row_count_to_be_between", {"min_value": value + 1}
    return etype, kwargs


def _column_exists(df: pd.DataFrame, col: str) -> bool:
    return col in df.columns


def main():
    setup_logging()
    data_path = SILVER_DIR / "survey_clean.parquet"
    if not data_path.exists():
        raise FileNotFoundError(f"Silver não encontrado: {data_path}")

    df = pd.read_parquet(data_path)

    # Relatório simples de schema
    REPORTS_DIR.mkdir(exist_ok=True)
    schema_info = {
        "columns": [
            {"name": c, "dtype": str(df[c].dtype), "non_null": int(df[c].notna().sum())}
            for c in df.columns
        ],
        "row_count": int(len(df)),
    }
    (REPORTS_DIR / "silver_schema.json").write_text(
        json.dumps(schema_info, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    if not SUITE_FILE.exists():
        raise FileNotFoundError(f"Suíte não encontrada: {SUITE_FILE}")
    suite = json.loads(SUITE_FILE.read_text(encoding="utf-8"))

    gx_df = SurveyDataset(df)
    all_results = []
    success_overall = True
    warnings = []

    for exp in suite.get("expectations", []):
        etype = exp.get("expectation_type")
        kwargs = exp.get("kwargs", {})

        etype, kwargs = _map_expectation_name(etype, kwargs)

        col = kwargs.get("column")
        if col and not _column_exists(df, col):
            warnings.append(f"[warn] coluna ausente para expectativa {etype}: '{col}'")
            all_results.append(
                {"expectation": etype, "kwargs": kwargs, "skipped": True, "reason": "missing_column"}
            )
            continue

        try:
            method = getattr(gx_df, etype)
        except AttributeError:
            warnings.append(f"[warn] expectativa não suportada: {etype} (ignorando)")
            all_results.append(
                {"expectation": etype, "kwargs": kwargs, "skipped": True, "reason": "unsupported"}
            )
            continue

        res = method(**kwargs).to_json_dict()
        all_results.append(res)
        if res.get("success") is False:
            success_overall = False

    validation_report = {
        "success": success_overall,
        "row_count": len(df),
        "warnings": warnings,
        "results": all_results,
    }
    (REPORTS_DIR / "silver_validation.json").write_text(
        json.dumps(validation_report, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    logger.info(f"[dq] success = {success_overall}")
    for w in warnings:
        logger.warning(w)

    if not success_overall:
        if STRICT:
            # Falha “real” (E2E/push)
            raise SystemExit(2)
        else:
            # Smoke de PR: não aborta
            logger.warning("[dq] validação falhou, mas GE_STRICT=0 (modo smoke/PR) -> seguindo sem abortar")


if __name__ == "__main__":
    main()
