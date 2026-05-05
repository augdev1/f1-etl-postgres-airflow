"""Ponto de entrada do pipeline F1."""

from config.settings import DEFAULT_SEASON
from scripts.pipeline import run_pipeline
from utils.logger import get_logger

logger = get_logger(__name__)


if __name__ == "__main__":
    try:
        logger.info("Iniciando pipeline F1 para a temporada %s...", DEFAULT_SEASON)
        metrics = run_pipeline(year=DEFAULT_SEASON)
        logger.info("Pipeline concluído com sucesso! Métricas: %s", metrics)
    except Exception as e:
        logger.error("Pipeline falhou: %s", e, exc_info=True)
        raise
