from pathlib import Path
import os

# paths
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR   = ROOT / "data"
RAW_DIR    = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR   = DATA_DIR / "gold"

for p in (RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR):
    p.mkdir(parents=True, exist_ok=True)

# dataset alvo (padrão = hearing wellness 2025)
KAGGLE_DATASET = os.getenv(
    "KAGGLE_DATASET",
    "adharshinikumar/2025-hearing-wellness-survey"
)
