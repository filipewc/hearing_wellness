[![CI](https://github.com/filipewc/hearing_wellness/actions/workflows/ci.yml/badge.svg)](https://github.com/filipewc/hearing_wellness/actions/workflows/ci.yml)

# hearing_wellness 

# Hearing Wellness Data Pipeline

Este repositório demonstra um **pipeline completo de engenharia de dados**, usando um dataset público do Kaggle sobre **bem-estar auditivo**.

## 🔄 Arquitetura do Pipeline

```
Raw (Kaggle CSV)
    ↓
Bronze (padronização de schema e colunas)
    ↓
Silver (limpeza, canônicos, deduplicação)
    ↓
Validation (Great Expectations)
    ↓
Gold (KPIs e tabelas analíticas prontas para BI)
```

## 📂 Estrutura do Projeto

```
hearing_wellness/
│
├── data/
│   ├── raw/       # Dados brutos baixados do Kaggle
│   ├── bronze/    # Dados padronizados
│   ├── silver/    # Dados limpos e canônicos
│   └── gold/      # Tabelas analíticas e KPIs
│
├── src/
│   ├── pipelines/ # Scripts ETL (download, bronze, silver, validate, gold)
│   └── utils/     # Funções auxiliares (colunas, schema map, etc.)
│
├── expectations/  # Suítes de validação (Great Expectations)
├── reports/       # Relatórios de schema e validação
├── run_pipeline.ps1   # Runner do pipeline (PowerShell)
├── requirements.txt   # Dependências
└── README.md
```

## ▶️ Como Executar

### Pré-requisitos
- Python 3.10+
- Conta no Kaggle e `kaggle.json` configurado em `%USERPROFILE%/.kaggle/kaggle.json`

### Passos
```powershell
# 1. Clonar repositório
git clone https://github.com/filipewc/hearing_wellness.git
cd hearing_wellness

# 2. Criar ambiente virtual
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 3. Instalar dependências
pip install -r requirements.txt

# 4. Executar pipeline
.
un_pipeline.ps1 -Fresh
```

## ✅ Funcionalidades

- **Download Kaggle**: baixa dados via [kagglehub](https://pypi.org/project/kagglehub/).  
- **Bronze**: padroniza colunas, normaliza textos.  
- **Silver**: remove duplicatas, cria colunas canônicas (`age`, `gender`, `hearing_issues`).  
- **Validation**: validações com [Great Expectations](https://greatexpectations.io/).  
- **Gold**: gera KPIs (`prevalence_overall_pct`, cortes por faixa etária e gênero).

## 📊 Próximos Passos

- Criar dashboard em Power BI/Streamlit/Tableau a partir das tabelas Gold.
- CI/CD com GitHub Actions.
- Publicação de artigo no LinkedIn mostrando o case end-to-end.

---
