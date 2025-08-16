param(
    [switch]$Fresh
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $here

# ativa venv
if (-not (Test-Path .\.venv\Scripts\Activate.ps1)) { throw "Venv não encontrada. Crie com: python -m venv .venv" }
.\.venv\Scripts\Activate.ps1

# opcional: limpar dados
if ($Fresh) {
    Write-Host ">> limpando data/*" -ForegroundColor Yellow
    Remove-Item -Recurse -Force .\data\raw\* -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force .\data\bronze\* -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force .\data\silver\* -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force .\data\gold\* -ErrorAction SilentlyContinue
}

# se quiser centralizar o cache do kagglehub no repo:
$env:KAGGLEHUB_CACHE = "$here\data\.kagglehub"

Write-Host ">> Etapa 1: Download Kaggle" -ForegroundColor Cyan
python -m src.pipelines.download_kaggle

Write-Host ">> Etapa 2: Bronze" -ForegroundColor Cyan
python -m src.pipelines.bronze_ingest

Write-Host ">> Etapa 3: Silver" -ForegroundColor Cyan
python -m src.pipelines.silver_clean

Write-Host "OK."
