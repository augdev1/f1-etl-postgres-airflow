"""Orquestrador do pipeline ETL F1 — pronto para integração com Airflow."""

import pandas as pd

from scripts.extract import get_data_from_api
from scripts.load import load_results, load_to_db
from scripts.transform import (
    transform_constructors,
    transform_drivers,
    transform_races,
    transform_results,
)
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Funções independentes (task-level) — ideais para operadores Airflow
# ---------------------------------------------------------------------------

def extract_races(year: int) -> list[dict]:
    """Task: extrai corridas da API."""
    logger.info("[Extract] Iniciando extração de corridas para %s.", year)
    return get_data_from_api("races", year)


def transform_races_task(raw_data: list[dict]) -> pd.DataFrame:
    """Task: transforma dados brutos de corridas."""
    logger.info("[Transform] Transformando corridas.")
    return transform_races(raw_data)


def load_races_task(df: pd.DataFrame) -> int:
    """Task: carrega corridas no banco (idempotente)."""
    logger.info("[Load] Carregando corridas.")
    return load_to_db(
        df,
        table_name="races",
        conflict_columns=["race_name", "date"],
        update_columns=["circuit", "country"],
    )


def extract_drivers(year: int) -> list[dict]:
    """Task: extrai pilotos da API."""
    logger.info("[Extract] Iniciando extração de pilotos para %s.", year)
    return get_data_from_api("drivers", year)


def transform_drivers_task(raw_data: list[dict]) -> pd.DataFrame:
    """Task: transforma dados brutos de pilotos."""
    logger.info("[Transform] Transformando pilotos.")
    return transform_drivers(raw_data)


def load_drivers_task(df: pd.DataFrame) -> int:
    """Task: carrega pilotos no banco (idempotente)."""
    logger.info("[Load] Carregando pilotos.")
    return load_to_db(
        df,
        table_name="drivers",
        conflict_columns=["name"],
        update_columns=["nationality"],
    )


def extract_constructors(year: int) -> list[dict]:
    """Task: extrai construtores da API."""
    logger.info("[Extract] Iniciando extração de construtores para %s.", year)
    return get_data_from_api("constructors", year)


def transform_constructors_task(raw_data: list[dict]) -> pd.DataFrame:
    """Task: transforma dados brutos de construtores."""
    logger.info("[Transform] Transformando construtores.")
    return transform_constructors(raw_data)


def load_constructors_task(df: pd.DataFrame) -> int:
    """Task: carrega construtores no banco (idempotente)."""
    logger.info("[Load] Carregando construtores.")
    return load_to_db(
        df,
        table_name="constructors",
        conflict_columns=["name"],
        update_columns=["nationality"],
    )


def extract_results(year: int) -> list[dict]:
    """Task: extrai resultados da API."""
    logger.info("[Extract] Iniciando extração de resultados para %s.", year)
    return get_data_from_api("results", year)


def transform_results_task(raw_data: list[dict]) -> pd.DataFrame:
    """Task: transforma dados brutos de resultados."""
    logger.info("[Transform] Transformando resultados.")
    return transform_results(raw_data)


def load_results_task(df: pd.DataFrame) -> int:
    """Task: carrega resultados no banco (idempotente com lookups)."""
    logger.info("[Load] Carregando resultados.")
    return load_results(df)


# ---------------------------------------------------------------------------
# Pipeline completo (orquestração sequencial)
# ---------------------------------------------------------------------------

def run_pipeline(year: int = 2023) -> dict:
    """
    Executa o pipeline ETL completo para uma temporada.

    Retorna um dicionário com métricas de carga de cada entidade.
    """
    metrics = {}

    # Races
    raw_races = extract_races(year)
    df_races = transform_races_task(raw_races)
    metrics["races"] = load_races_task(df_races)

    # Drivers
    raw_drivers = extract_drivers(year)
    df_drivers = transform_drivers_task(raw_drivers)
    metrics["drivers"] = load_drivers_task(df_drivers)

    # Constructors
    raw_constructors = extract_constructors(year)
    df_constructors = transform_constructors_task(raw_constructors)
    metrics["constructors"] = load_constructors_task(df_constructors)

    # Results (depende das entidades anteriores)
    raw_results = extract_results(year)
    df_results = transform_results_task(raw_results)
    metrics["results"] = load_results_task(df_results)

    logger.info("Pipeline finalizado. Métricas: %s", metrics)
    return metrics
