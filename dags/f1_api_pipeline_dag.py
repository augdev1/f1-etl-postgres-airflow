"""DAG de extracao e carga de dados da API F1 para PostgreSQL via Airflow 3.x SDK."""

import json
import sys

# Garante que a raiz do projeto esteja no path do Python
sys.path.insert(0, "/opt/airflow")

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

from scripts.extract import get_data_from_api
from utils.logger import get_logger

logger = get_logger("f1_pipeline")

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="f1_official_api_extraction",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["f1", "api", "ads"],
)
def f1_api_pipeline():
    """Orquestra a extracao de dados da API F1 e carga no PostgreSQL."""

    @task
    def task_extract_api():
        """Extrai dados brutos da API F1 via scripts.extract."""
        logger.info("Iniciando extracao da API de F1.")
        data = get_data_from_api(entity="results", year=2024, limit=1000)
        logger.info("Extracao finalizada: %s registros obtidos.", len(data))
        return data

    @task
    def task_load_postgres(data: list[dict]) -> int:
        """Serializa os dados em JSON e insere no f1db via PostgresHook."""
        pg_hook = PostgresHook(postgres_conn_id="postgres_f1")

        records = []
        for row in data:
            payload = json.dumps(row, ensure_ascii=False)
            records.append((payload,))

        sql = "INSERT INTO f1_results (payload) VALUES (%s) ON CONFLICT DO NOTHING;"

        inserted = 0
        for record in records:
            pg_hook.run(sql, parameters=record)
            inserted += 1

        logger.info(
            "Carga finalizada: %s registros processados com sucesso no f1db.",
            inserted,
        )
        return inserted

    # Orquestracao
    extracted_data = task_extract_api()
    task_load_postgres(extracted_data)


f1_api_pipeline()
