from pathlib import Path
import shutil
import kagglehub
from src.settings import RAW_DIR, KAGGLE_DATASET

def main():
    print(f"[download] dataset = {KAGGLE_DATASET}")
    # baixa para o cache local do kagglehub e retorna a pasta
    src_path = Path(kagglehub.dataset_download(KAGGLE_DATASET))
    print(f"[download] cache path: {src_path}")

    csvs = list(src_path.rglob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"Nenhum CSV encontrado em {src_path}")

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for csv in csvs:
        dest = RAW_DIR / csv.name
        shutil.copy2(csv, dest)
        print(f"[download] copiado: {csv.name} -> {dest}")

    print("[download] OK")

if __name__ == "__main__":
    main()
