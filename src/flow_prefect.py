from __future__ import annotations

import os
import sys
import shutil
import subprocess
from pathlib import Path
from typing import Dict, Optional

from prefect import flow, task, get_run_logger


def _repo_root() -> Path:
    # src/flow_prefect.py -> raiz do repo
    return Path(__file__).resolve().parents[1]


def _python_exe() -> str:
    # usa o interpretador do venv atual
    return sys.executable


@task(name="Prepare workspace", retries=0, log_prints=True)
def prepare_workspace(fresh: bool = True) -> Dict[str, str]:
    """
    - Limpa data/* quando fresh=True
    - Define envs úteis (KAGGLEHUB_CACHE e KAGGLE_DATASET)
    """
    logger = get_run_logger()
    root = _repo_root()
    data_dir = root / "data"
    for sub in ["raw", "bronze", "silver", "gold"]:
        (data_dir / sub).mkdir(parents=True, exist_ok=True)

    if fresh:
        logger.info("Limpando diretórios de dados (fresh=True)...")
        for sub in ["raw", "bronze", "silver", "gold"]:
            target = data_dir / sub
            for child in target.glob("*"):
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    try:
                        child.unlink()
                    except Exception:
                        pass

    kaggle_cache = data_dir / ".kagglehub"
    kaggle_cache.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["KAGGLEHUB_CACHE"] = str(kaggle_cache)
    env.setdefault("KAGGLE_DATASET", "adharshinikumar/2025-hearing-wellness-survey")

    logger.info(f"KAGGLEHUB_CACHE={env['KAGGLEHUB_CACHE']}")
    logger.info(f"KAGGLE_DATASET={env['KAGGLE_DATASET']}")
    logger.info(f"Python usado: {_python_exe()}")
    return env


@task(name="Run step", retries=2, retry_delay_seconds=10, log_prints=True)
def run_step(description: str, module: str, extra_env: Optional[Dict[str, str]] = None) -> None:
    """
    Executa `python -m <module>` usando o Python do venv e encaminha stdout/stderr aos logs.
    """
    logger = get_run_logger()
    cmd = [_python_exe(), "-m", module]
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    # reforça o PATH para priorizar o venv no Windows
    venv_scripts = Path(_python_exe()).parent
    env["PATH"] = str(venv_scripts) + os.pathsep + env.get("PATH", "")

    logger.info(f">>> {description}: {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=str(_repo_root()), capture_output=True, text=True, env=env)
    if proc.stdout:
        logger.info(proc.stdout.strip())
    if proc.stderr:
        logger.warning(proc.stderr.strip())
    if proc.returncode != 0:
        raise RuntimeError(f"Falha na etapa '{description}' (exit={proc.returncode})")


@flow(name="hearing_wellness_pipeline")
def hearing_pipeline(kaggle_dataset: str = "adharshinikumar/2025-hearing-wellness-survey",
                     fresh: bool = True) -> None:
    """
    Orquestra o pipeline:
      1) Download Kaggle
      2) Bronze
      3) Silver
      4) Data Quality (Great Expectations)
      5) Gold
    """
    logger = get_run_logger()
    env = prepare_workspace.submit(fresh=fresh).result()
    env["KAGGLE_DATASET"] = kaggle_dataset

    run_step.submit("Download Kaggle", "src.pipelines.download_kaggle", env).result()
    run_step.submit("Bronze Ingest", "src.pipelines.bronze_ingest", env).result()
    run_step.submit("Silver Clean", "src.pipelines.silver_clean", env).result()
    run_step.submit("Data Validation", "src.pipelines.silver_validate", env).result()
    run_step.submit("Gold Build", "src.pipelines.gold_build", env).result()

    logger.info("Pipeline concluído com sucesso. Artefatos em data/gold/.")


if __name__ == "__main__":
    hearing_pipeline()
