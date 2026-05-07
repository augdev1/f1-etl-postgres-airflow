"""Camada de extração de dados da API Ergast (Jolpi mirror)."""

import time

import requests

from config.settings import API_BASE_URL, API_TIMEOUT
from utils.logger import get_logger

logger = get_logger(__name__)


def _fetch(endpoint: str) -> dict:
    """Faz uma requisição GET à API com retry automático e retorna o JSON validado."""
    url = f"{API_BASE_URL}{endpoint}"
    logger.info("Requisitando URL: %s", url)

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url, timeout=API_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            logger.warning(
                "Tentativa %s/%s falhou para %s: %s",
                attempt,
                max_attempts,
                url,
                e,
            )
            if attempt == max_attempts:
                logger.error("Maximo de tentativas excedido para %s", url)
                raise
            time.sleep(2)
        except requests.exceptions.RequestException as e:
            logger.error("Erro irrecuperavel na requisicao para %s: %s", url, e)
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
    if entity == "results":
        # Grid completo: itera sobre cada rodada para extrair todos os pilotos
        schedule_data = _fetch(f"/{year}.json")
        races_list = schedule_data["MRData"]["RaceTable"]["Races"]

        all_races_with_results = []
        for race in races_list:
            round_number = race.get("round")
            race_name = race.get("raceName", "Desconhecida")
            if not round_number:
                logger.warning(
                    "Corrida '%s' sem numero de rodada; pulando.",
                    race_name,
                )
                continue

            try:
                round_data = _fetch(f"/{year}/{round_number}/results.json")
                round_races = round_data["MRData"]["RaceTable"]["Races"]
                if not round_races:
                    logger.warning(
                        "Rodada %s ('%s') retornou vazio.",
                        round_number,
                        race_name,
                    )
                    continue

                race_detail = round_races[0]
                if "Results" not in race_detail:
                    logger.warning(
                        "Rodada %s ('%s') retornou dados malformados "
                        "(campo 'Results' ausente); pulando.",
                        round_number,
                        race_name,
                    )
                    continue

                results_list = race_detail["Results"]
                if not results_list:
                    logger.warning(
                        "Rodada %s ('%s') retornou lista de resultados vazia.",
                        round_number,
                        race_name,
                    )
                    continue

                for driver_result in results_list:
                    record = {
                        "season": race_detail.get("season"),
                        "round": race_detail.get("round"),
                        "raceName": race_detail.get("raceName"),
                        "date": race_detail.get("date"),
                        "Results": driver_result,
                    }
                    all_races_with_results.append(record)
            except Exception:
                # Erros já logados pelo _fetch; segue para próxima rodada
                continue

        logger.info(
            "Grid completo extraido: %s registros (pilotos) de %s corridas.",
            len(all_races_with_results),
            len(races_list),
        )
        return all_races_with_results

    endpoints = {
        "races": f"/{year}.json",
        "drivers": f"/{year}/drivers.json",
        "constructors": f"/{year}/constructors.json",
    }

    if entity not in endpoints:
        raise ValueError(f"Entidade desconhecida: {entity}")

    data = _fetch(endpoints[entity])

    if entity == "races":
        return data["MRData"]["RaceTable"]["Races"]
    if entity == "drivers":
        return data["MRData"]["DriverTable"]["Drivers"]
    if entity == "constructors":
        return data["MRData"]["ConstructorTable"]["Constructors"]

    return []
