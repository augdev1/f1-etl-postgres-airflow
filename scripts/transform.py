"""Camada de transformação dos dados brutos em DataFrames limpos."""

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


def transform_races(raw_data: list[dict]) -> pd.DataFrame:
    """Transforma dados brutos de corridas em um DataFrame limpo."""
    records = []
    for race in raw_data:
        records.append({
            "race_name": race["raceName"],
            "circuit": race["Circuit"]["circuitName"],
            "country": race["Circuit"]["Location"]["country"],
            "date": race["date"],
        })
    df = pd.DataFrame(records)
    logger.info("Transformadas %s corridas.", len(df))
    return df


def transform_drivers(raw_data: list[dict]) -> pd.DataFrame:
    """Transforma dados brutos de pilotos em um DataFrame limpo."""
    records = []
    for driver in raw_data:
        records.append({
            "name": f"{driver['givenName']} {driver['familyName']}",
            "nationality": driver["nationality"],
        })
    df = pd.DataFrame(records)
    logger.info("Transformados %s pilotos.", len(df))
    return df


def transform_constructors(raw_data: list[dict]) -> pd.DataFrame:
    """Transforma dados brutos de construtores em um DataFrame limpo."""
    records = []
    for team in raw_data:
        records.append({
            "name": team["name"],
            "nationality": team["nationality"],
        })
    df = pd.DataFrame(records)
    logger.info("Transformados %s construtores.", len(df))
    return df


def transform_results(raw_data: list[dict]) -> pd.DataFrame:
    """
    Transforma dados brutos de resultados em um DataFrame limpo.
    Requer lookups de race_id, driver_id e constructor_id que serão
    resolvidos na camada de carga (load).
    """
    records = []
    for race in raw_data:
        race_name = race["raceName"]
        for res in race["Results"]:
            records.append({
                "race_name": race_name,
                "driver_name": f"{res['Driver']['givenName']} {res['Driver']['familyName']}",
                "constructor_name": res["Constructor"]["name"],
                "position": int(res["position"]),
                "points": float(res["points"]),
            })
    df = pd.DataFrame(records)
    logger.info("Transformados %s resultados.", len(df))
    return df
