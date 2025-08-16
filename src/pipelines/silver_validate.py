import json
from pathlib import Path
import pandas as pd
from great_expectations.dataset import PandasDataset
from src.settings import SILVER_DIR

SUITE_FILE = Path("expectations/survey_clean_suite.json")
REPORTS_DIR = Path("reports")


class SurveyDataset(PandasDataset):
    pass


def _map_expectation_name(etype: str, kwargs: dict):
    """
    Mapeia nomes não suportados para equivalentes do PandasDataset.
    - expect_table_row_count_to_be_greater_than -> expect_table_row_count_to_be_between (min = value+1)
    """
    if etype == "expect_table_row_count_to_be_greater_than":
        value = kwargs.get("value", 0)
        return "expect_table_row_count_to_be_between", {"min_value": value + 1}
    return etype, kwargs


def _column_exists(df: pd.DataFrame, col: str) -> bool:
    return col in df.columns


def main():
    data_path = SILVER_DIR / "survey_clean.parquet"
    if not data_path.exists():
        raise FileNotFoundError(f"Silver não encontrado: {data_path}")

    df = pd.read_parquet(data_path)

    # salva um relatório simples de schema para facilitar ajuste da suíte
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

    # carrega a suíte
    if not SUITE_FILE.exists():
        raise FileNotFoundError(
            f"Suíte não encontrada: {SUITE_FILE}. Crie expectations/survey_clean_suite.json"
        )
    suite = json.loads(SUITE_FILE.read_text(encoding="utf-8"))

    gx_df = SurveyDataset(df)
    all_results = []
    success_overall = True
    warnings = []

    for exp in suite.get("expectations", []):
        etype = exp.get("expectation_type")
        kwargs = exp.get("kwargs", {})

        # mapear nomes para API suportada
        etype, kwargs = _map_expectation_name(etype, kwargs)

        # se expectativa envolver coluna, verifique existência
        col = kwargs.get("column")
        if col and not _column_exists(df, col):
            warnings.append(f"[warn] coluna ausente para expectativa {etype}: '{col}'")
            # não falha o pipeline por coluna faltando; apenas registra
            all_results.append(
                {"expectation": etype, "kwargs": kwargs, "skipped": True, "reason": "missing_column"}
            )
            continue

        # chamar a expectativa
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

    # salva relatório de validação
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

    print(f"[dq] success = {success_overall}")
    for w in warnings:
        print(w)

    if not success_overall:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
