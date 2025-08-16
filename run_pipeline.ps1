Write-Host ">> Etapa 1: Download Kaggle" -ForegroundColor Cyan
python -m src.pipelines.download_kaggle
if ($LASTEXITCODE -ne 0) { throw "Falha na Etapa 1 (download)" }

Write-Host ">> Etapa 2: Bronze" -ForegroundColor Cyan
python -m src.pipelines.bronze_ingest
if ($LASTEXITCODE -ne 0) { throw "Falha na Etapa 2 (bronze)" }

Write-Host ">> Etapa 3: Silver" -ForegroundColor Cyan
python -m src.pipelines.silver_clean
if ($LASTEXITCODE -ne 0) { throw "Falha na Etapa 3 (silver)" }

Write-Host ">> Etapa 4: Data Quality (Great Expectations)" -ForegroundColor Cyan
python -m src.pipelines.silver_validate
if ($LASTEXITCODE -ne 0) { throw "Falha na Etapa 4 (data quality)" }

Write-Host ">> Etapa 5: Gold" -ForegroundColor Cyan
python -m src.pipelines.gold_build
if ($LASTEXITCODE -ne 0) { throw "Falha na Etapa 5 (gold)" }

Write-Host "OK."
