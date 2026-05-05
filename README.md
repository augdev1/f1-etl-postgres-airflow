# F1 Data Pipeline

> ETL modular e idempotente para dados da Formula 1, construido com Python, pandas, SQLAlchemy Core e PostgreSQL. Pronto para orquestracao com Apache Airflow.

---

## Visao Geral

Este projeto extrai dados da API Ergast (via mirror Jolpi), transforma-os em DataFrames pandas e carrega-os de forma idempotente em um banco PostgreSQL. A arquitetura e modular, com camadas claras de Extração, Transformacao e Carga (ETL), facilitando manutencao, testes e integracao com orquestradores.

## Arquitetura

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Ergast    │────>│   Extract   │────>│  Transform  │────>│    Load     │
│   API       │     │  (requests) │     │  (pandas)   │     │(SQLAlchemy  │
└─────────────┘     └─────────────┘     └─────────────┘     │    Core)    │
                                                            └──────┬──────┘
                                                                   │
                                                            ┌──────▼──────┐
                                                            │  PostgreSQL │
                                                            └─────────────┘
```

## Estrutura do Projeto

```
f1-pipeline/
├── config/
│   ├── __init__.py
│   └── settings.py              # Configuracoes centralizadas (DB, API)
├── utils/
│   ├── __init__.py
│   ├── db.py                    # Conexoes SQLAlchemy Engine
│   └── logger.py                # Logger padronizado
├── scripts/
│   ├── __init__.py
│   ├── extract.py               # Extracao da API Ergast
│   ├── transform.py             # Transformacao com pandas
│   ├── load.py                  # Carga idempotente via SQLAlchemy Core
│   ├── pipeline.py              # Orquestracao ETL (Airflow-ready)
│   └── audit.py                 # Auditoria de qualidade de dados
├── data/                        # Cache local de dados (opcional)
├── main.py                      # Ponto de entrada do pipeline
├── pyproject.toml               # Dependencias do projeto
├── .env                         # Variaveis de ambiente (nao versionado)
└── README.md
```

## Pre-requisitos

- Python >= 3.12
- PostgreSQL (via Docker ou instalado)
- [uv](https://docs.astral.sh/uv/) ou pip

## Instalacao

```bash
# Clone o repositorio
git clone <repo-url>
cd f1-pipeline

# Instale as dependencias
uv sync
# ou
pip install -e .
```

## Configuracao

Crie um arquivo `.env` na raiz do projeto:

```env
DB_PASSWORD=sua_senha_aqui
```

Outras variaveis sao opcionais (valores padrao definidos em `config/settings.py`):

```env
DB_HOST=localhost
DB_NAME=f1db
DB_USER=aug
DB_PORT=5432
```

> **Nota:** O arquivo `.env` e carregado automaticamente pelo `python-dotenv` em `config/settings.py`.

## Uso

### Executar o pipeline completo

```bash
python main.py
```

### Executar auditoria de dados

```bash
python -c "from scripts.audit import audit_results; audit_results()"
```

### Executar tasks isoladamente

```python
from scripts.pipeline import extract_races, transform_races_task, load_races_task

raw = extract_races(year=2023)
df = transform_races_task(raw)
load_races_task(df)
```

## Preparado para Apache Airflow

Todas as funcoes em `scripts/pipeline.py` sao **puras e independentes** — recebem parametros e retornam objetos serializaveis. Isso as torna ideais para operadores `@task` do Airflow.

### Exemplo de DAG

```python
from airflow.decorators import dag, task
from datetime import datetime

from scripts.pipeline import (
    extract_races, transform_races_task, load_races_task,
    extract_drivers, transform_drivers_task, load_drivers_task,
    extract_constructors, transform_constructors_task, load_constructors_task,
    extract_results, transform_results_task, load_results_task,
)


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["f1", "etl"],
)
def f1_etl_dag():

    @task
    def extract_races_task(year: int):
        return extract_races(year)

    @task
    def transform_races(raw_data):
        return transform_races_task(raw_data)

    @task
    def load_races(df):
        return load_races_task(df)

    @task
    def extract_drivers_task(year: int):
        return extract_drivers(year)

    @task
    def transform_drivers(raw_data):
        return transform_drivers_task(raw_data)

    @task
    def load_drivers(df):
        return load_drivers_task(df)

    @task
    def extract_constructors_task(year: int):
        return extract_constructors(year)

    @task
    def transform_constructors(raw_data):
        return transform_constructors_task(raw_data)

    @task
    def load_constructors(df):
        return load_constructors_task(df)

    @task
    def extract_results_task(year: int):
        return extract_results(year)

    @task
    def transform_results(raw_data):
        return transform_results_task(raw_data)

    @task
    def load_results(df):
        return load_results_task(df)

    # Dependencias
    races = load_races(transform_races(extract_races_task(2023)))
    drivers = load_drivers(transform_drivers(extract_drivers_task(2023)))
    constructors = load_constructors(transform_constructors(extract_constructors_task(2023)))
    results = load_results(transform_results(extract_results_task(2023)))

    results >> [races, drivers, constructors]


f1_etl_dag()
```

## Boas Praticas Implementadas

| Pratica | Implementacao |
|---------|---------------|
| **Idempotencia** | `ON CONFLICT ... DO UPDATE` em todas as cargas |
| **Bulk Insert** | `df.to_dict(orient="records")` + `conn.execute(text(query), records)` |
| **Atomicidade** | Transacoes via `with engine.begin() as conn:` |
| **Seguranca** | Credenciais via variaveis de ambiente (`.env`) |
| **Logging** | Logger centralizado com timestamps e niveis |
| **Modularidade** | Separacao clara de Extract, Transform, Load |
| **Type Hints** | Todas as funcoes tipadas para legibilidade |

## Tecnologias

- [Python](https://www.python.org/) >= 3.12
- [pandas](https://pandas.pydata.org/)
- [SQLAlchemy](https://www.sqlalchemy.org/) Core
- [psycopg2](https://www.psycopg.org/)
- [PostgreSQL](https://www.postgresql.org/)
- [Apache Airflow](https://airflow.apache.org/) (ready)

## Licenca

Este projeto esta licenciado sob a licenca MIT.
