"""Camada de extração de dados da API Ergast (Jolpi mirror)."""

import requests

from config.settings import API_BASE_URL, API_TIMEOUT
from utils.logger import get_logger

logger = get_logger(__name__)


def _fetch(endpoint: str) -> dict:
    """Faz uma requisição GET à API e retorna o JSON validado."""
    url = f"{API_BASE_URL}{endpoint}"
    logger.info("Requisitando URL: %s", url)

    try:
        response = requests.get(url, timeout=API_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error("Erro na requisição para %s: %s", url, e)
        raise


def get_data_from_api(entity: str, year: int, limit: int = 1000) -> list[dict]:
    """
    Extrai dados brutos da API para uma entidade específica.

    Args:
        entity: Tipo de entidade ('races', 'drivers', 'constructors', 'results').
        year: Ano da temporada.
        limit: Limite de registros (relevante para results).

    Returns:
        Lista de dicionários com os dados brutos da API.
    """
    endpoints = {
        "races": f"/{year}.json",
        "drivers": f"/{year}/drivers.json",
        "constructors": f"/{year}/constructors.json",
        "results": f"/{year}/results.json?limit={limit}",
    }

    if entity not in endpoints:
        raise ValueError(f"Entidade desconhecida: {entity}")

    data = _fetch(endpoints[entity])

    # Navegação no JSON varia conforme a entidade
    if entity == "results":
        return data["MRData"]["RaceTable"]["Races"]
    if entity == "races":
        return data["MRData"]["RaceTable"]["Races"]
    if entity == "drivers":
        return data["MRData"]["DriverTable"]["Drivers"]
    if entity == "constructors":
        return data["MRData"]["ConstructorTable"]["Constructors"]

    return []
