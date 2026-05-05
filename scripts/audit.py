"""Auditoria de qualidade de dados no banco PostgreSQL."""

import pandas as pd

from utils.db import get_sqlalchemy_engine
from utils.logger import get_logger

logger = get_logger(__name__)


def audit_results() -> dict:
    """
    Executa auditoria na tabela results verificando:
    - Duplicatas por (race_id, driver_id)
    - Consistência de pontos negativos
    - Resumo estatístico

    Returns:
        Dicionário com métricas da auditoria.
    """
    logger.info("Iniciando auditoria na tabela 'results'...")
    engine = get_sqlalchemy_engine()

    try:
        df = pd.read_sql("SELECT * FROM results", engine)
    except Exception as e:
        logger.error("Erro ao ler tabela results: %s", e)
        raise

    metrics = {
        "total_records": int(len(df)),
        "total_points": float(df["points"].sum()),
        "duplicate_rows": 0,
        "negative_points": 0,
    }

    # 1. Duplicatas
    duplicate_mask = df.duplicated(subset=["race_id", "driver_id"], keep=False)
    duplicates = df[duplicate_mask]
    if not duplicates.empty:
        metrics["duplicate_rows"] = int(len(duplicates))
        logger.warning("Encontradas %s linhas duplicadas por (race_id, driver_id).", len(duplicates))
    else:
        logger.info("Nenhuma duplicata encontrada por (race_id, driver_id).")

    # 2. Pontos negativos
    invalid_points = df[df["points"] < 0]
    if not invalid_points.empty:
        metrics["negative_points"] = int(len(invalid_points))
        logger.warning("Encontrados %s registros com pontos negativos.", len(invalid_points))
    else:
        logger.info("Todos os pontos são válidos.")

    logger.info("Auditoria finalizada. Métricas: %s", metrics)
    return metrics
