# F1 Data Pipeline

> ETL modular e idempotente para dados da Formula 1, construido com Python 3.12+, pandas, SQLAlchemy Core e PostgreSQL. Orquestracao via **Apache Airflow 3.x** e visualizacao via **Metabase** — tudo provisionado via Docker Compose.

---

## Visao Geral

Este projeto extrai dados da API Ergast (via mirror Jolpi), transforma-os em DataFrames pandas e carrega-os de forma idempotente em um banco PostgreSQL. A arquitetura e modular, com camadas claras de **Extracao**, **Transformacao** e **Carga** (ETL), facilitando manutencao, testes e integracao com orquestradores.

A infraestrutura completa (Airflow, PostgreSQL, Redis, Metabase) e provisionada via **Docker Compose** com volumes mapeados para desenvolvimento local.

## Arquitetura

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Jolpi API  │────>│   Extract   │────>│  Transform  │────>│    Load     │
│ (Ergast)    │     │  (requests) │     │  (pandas)   │     │(SQLAlchemy  │
└─────────────┘     └─────────────┘     └─────────────┘     │    Core)    │
                                                            └──────┬──────┘
                                                                   │
                                                            ┌──────▼──────┐
                                                            │  PostgreSQL │
                                                            │   (f1db)    │
                                                            └──────┬──────┘
                                                                   │
                                                            ┌──────▼──────┐
                                                            │  Metabase   │
                                                            └─────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│                        Apache Airflow 3.x (Docker)                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │
│  │  API Server │  │  Scheduler  │  │  Worker     │  │  DagProc    │   │
│  │   :8080     │  │             │  │  (Celery)   │  │             │   │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │
│                                Redis :6379                                │
└──────────────────────────────────────────────────────────────────────────┘
```

## Estrutura do Projeto

```
f1-pipeline/
├── config/
│   ├── __init__.py
│   ├── settings.py              # Configuracoes centralizadas (DB, API)
│   └── airflow.cfg              # Configuracao customizada do Airflow
├── dags/
│   └── f1_api_pipeline_dag.py   # DAG Airflow 3.x (SDK decorators)
├── env/
│   ├── .env                     # Variaveis do Airflow (NAO versionar)
│   └── .env.example             # Template para env/.env
├── postgres-init/
│   └── 01-init.sql              # Criacao automatica do banco f1db
├── scripts/
│   ├── __init__.py
│   ├── audit.py                 # Auditoria de qualidade de dados
│   ├── extract.py               # Extracao da API Ergast (Jolpi)
│   ├── load.py                  # Carga idempotente via SQLAlchemy Core
│   ├── pipeline.py              # Orquestracao ETL (Airflow-ready)
│   └── transform.py             # Transformacao com pandas
├── utils/
│   ├── __init__.py
│   ├── db.py                    # Conexoes SQLAlchemy + psycopg2
│   └── logger.py                # Logger padronizado
├── .env.example                 # Template de variaveis da aplicacao
├── .gitignore
├── docker-compose.yaml          # Stack completa (Airflow + Postgres + Redis + Metabase)
├── main.py                      # Ponto de entrada do pipeline (execucao local)
├── pyproject.toml               # Dependencias do projeto
└── README.md
```

## Pre-requisitos

- [Docker](https://www.docker.com/) e Docker Compose v2+
- Python >= 3.12 (para execucao local fora do Docker)
- WSL2 (recomendado para Windows)

## Infraestrutura (Docker Compose)

O `docker-compose.yaml` provisiona toda a stack:

| Servico | Imagem | Porta | Descricao |
|---------|--------|-------|-----------|
| `postgres` | `postgres:16` | `5432` | Banco de metadados do Airflow + `f1db` |
| `redis` | `redis:7.2-bookworm` | `6379` | Broker do CeleryExecutor |
| `airflow-apiserver` | `apache/airflow:3.2.1` | `8080` | Interface web e API REST |
| `airflow-scheduler` | `apache/airflow:3.2.1` | — | Agendamento e parsing de DAGs |
| `airflow-dag-processor` | `apache/airflow:3.2.1` | — | Processamento assincrono de DAGs |
| `airflow-worker` | `apache/airflow:3.2.1` | — | Execucao de tarefas Celery |
| `metabase` | `metabase/metabase:latest` | `3000` | Visualizacao de dados |

### Subir a stack

```bash
# 1. Copie os templates de ambiente
cp .env.example .env
cp env/.env.example env/.env

# 2. Suba a infraestrutura
docker compose up -d

# 3. Crie a conexao PostgreSQL no Airflow
docker compose exec airflow-apiserver airflow connections add 'postgres_f1' \
  --conn-type 'postgres' \
  --conn-login 'airflow' \
  --conn-password 'airflow' \
  --conn-host 'postgres' \
  --conn-port '5432' \
  --conn-schema 'f1db'
```

### Acessos

| Servico | URL | Credenciais |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | `airflow` / `airflow` |
| Metabase | http://localhost:3000 | configurar no primeiro acesso |
| PostgreSQL | `localhost:5432` | `airflow` / `airflow` |

### Bancos de dados

| Banco | Usuario | Senha | Uso |
|-------|---------|-------|-----|
| `airflow` | `airflow` | `airflow` | Metadados do Airflow |
| `f1db` | `airflow` | `airflow` | Dados do pipeline F1 |

O banco `f1db` e criado automaticamente na inicializacao do container via `postgres-init/01-init.sql`.

## Configuracao

### Variaveis de ambiente da aplicacao

Copie o template na raiz do projeto e preencha os valores:

```bash
cp .env.example .env
```

Exemplo para execucao local com Docker Compose:

```env
DB_HOST=localhost
DB_NAME=f1db
DB_USER=airflow
DB_PASSWORD=airflow
DB_PORT=5432
```

> Carregado automaticamente pelo `python-dotenv` em `config/settings.py`.

### Variaveis de ambiente do Airflow

Copie o template na pasta `env/`:

```bash
cp env/.env.example env/.env
```

Preencha obrigatoriamente:

| Variavel | Valor recomendado | Descricao |
|----------|-------------------|-----------|
| `AIRFLOW_UID` | `50000` (ou `$(id -u)` no Linux) | UID do container |
| `AIRFLOW_PROJ_DIR` | `.` | Diretorio base dos volumes |
| `AIRFLOW_IMAGE_NAME` | `apache/airflow:3.2.1` | Imagem do Airflow |
| `FERNET_KEY` | Base64 de 32 bytes | Criptografia de variaveis |
| `AIRFLOW__CORE__FERNET_KEY` | Mesmo valor acima | Exposicao para o Airflow |
| `PYTHONPATH` | `/opt/airflow` | Path para importar `scripts/` e `utils/` |

> **Atenção:** Nunca versione o arquivo `env/.env` (ja esta no `.gitignore`).

### Gerar FERNET_KEY

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Uso

### Executar o pipeline completo (local)

```bash
python main.py
```

### Executar auditoria de qualidade

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

## Apache Airflow 3.x

### DAG: `f1_official_api_extraction`

A DAG principal utiliza a **SDK 3.x** (`airflow.sdk`) com decorators `@dag` e `@task`:

```python
from airflow.sdk import dag, task;
from airflow.providers.postgres.hooks.postgres import PostgresHook;

@dag(
    dag_id="f1_official_api_extraction",
    schedule="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["f1", "api", "ads"],
)
def f1_api_pipeline():
    ...
```

**Caracteristicas:**
- Importa modulos customizados (`scripts/`, `utils/`) via `sys.path.insert(0, '/opt/airflow')`
- Usa `PostgresHook` com `postgres_conn_id="postgres_f1"`
- Serializa dados em JSON via `json.dumps()` para coluna `JSONB`
- Carga idempotente com `ON CONFLICT DO NOTHING`

### Como o Airflow encontra os modulos `scripts` e `utils`

1. **Volume no docker-compose:** `utils/` e `scripts/` sao montados em `/opt/airflow/`
2. **PYTHONPATH:** definido como `/opt/airflow` no `env/.env`
3. **Fallback na DAG:** `sys.path.insert(0, '/opt/airflow')` no topo do arquivo
4. **`__init__.py`:** presentes em ambos os pacotes

## Boas Praticas Implementadas

| Pratica | Implementacao |
|---------|---------------|
| **Idempotencia** | `ON CONFLICT ... DO UPDATE/DO NOTHING` em todas as cargas |
| **Bulk Insert** | `df.to_dict(orient="records")` + `conn.execute(text(query), records)` |
| **Atomicidade** | Transacoes via `with engine.begin() as conn:` |
| **Seguranca** | Credenciais em `.env` (ignorado pelo Git) |
| **Logging** | Logger centralizado com timestamp e nivel (`utils/logger.py`) |
| **Modularidade** | Separação clara de Extract, Transform, Load, Config, Utils |
| **Type Hints** | Todas as funcoes tipadas para legibilidade e IDE |
| **Airflow 3.x** | SDK moderno (`airflow.sdk`) evitando imports depreciados |
| **Docker Healthchecks** | Todos os servicos possuem healthchecks configurados |
| **Init SQL** | Banco `f1db` criado automaticamente na subida do Postgres |

## Metabase

No primeiro acesso (`http://localhost:3000`), configure a conexao:

| Campo | Valor |
|-------|-------|
| Tipo | PostgreSQL |
| Host | `postgres` (nome do servico Docker) |
| Porta | `5432` |
| Database | `f1db` |
| Usuario | `airflow` |
| Senha | `airflow` |

## Tecnologias

- [Python](https://www.python.org/) >= 3.12
- [pandas](https://pandas.pydata.org/)
- [SQLAlchemy](https://www.sqlalchemy.org/) Core
- [psycopg2-binary](https://www.psycopg.org/)
- [PostgreSQL](https://www.postgresql.org/) 16
- [Apache Airflow](https://airflow.apache.org/) 3.2.1
- [Metabase](https://www.metabase.com/)
- [Docker](https://www.docker.com/) & Docker Compose
- [Redis](https://redis.io/) 7.2
- [python-dotenv](https://saurabh-kumar.com/python-dotenv/)

## Licenca

Este projeto esta licenciado sob a licenca MIT.
