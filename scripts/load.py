"""Camada de carga (load) dos DataFrames para o PostgreSQL via SQLAlchemy Core."""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from utils.db import get_sqlalchemy_engine
from utils.logger import get_logger

logger = get_logger(__name__)


def _get_lookups(conn) -> tuple[dict, dict, dict]:
    """Obtém mapeamentos de nome -> id para races, drivers e constructors."""
    race_rows = conn.execute(text("SELECT race_name, race_id FROM races")).mappings().all()
    races = {row["race_name"]: row["race_id"] for row in race_rows}

    driver_rows = conn.execute(text("SELECT name, driver_id FROM drivers")).mappings().all()
    drivers = {row["name"]: row["driver_id"] for row in driver_rows}

    constructor_rows = conn.execute(text("SELECT name, constructor_id FROM constructors")).mappings().all()
    constructors = {row["name"]: row["constructor_id"] for row in constructor_rows}

    return races, drivers, constructors


def load_to_db(
    df: pd.DataFrame,
    table_name: str,
    conflict_columns: list[str] | None = None,
    update_columns: list[str] | None = None,
) -> int:
    """
    Carrega um DataFrame para uma tabela PostgreSQL via SQLAlchemy Core (bulk).

    Args:
        df: DataFrame com os dados normalizados.
        table_name: Nome da tabela de destino.
        conflict_columns: Colunas para cláusula ON CONFLICT (idempotência).
        update_columns: Colunas para UPDATE em caso de conflito.

    Returns:
        Número de registros enviados para carga.
    """
    engine = get_sqlalchemy_engine()
    columns = list(df.columns)
    column_names = ", ".join(columns)
    placeholders = ", ".join([f":{col}" for col in columns])

    query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

    if conflict_columns and update_columns:
        conflict = ", ".join(conflict_columns)
        updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        query += f" ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
    elif conflict_columns:
        conflict = ", ".join(conflict_columns)
        query += f" ON CONFLICT ({conflict}) DO NOTHING"

    records = df.to_dict(orient="records")

    try:
        with engine.begin() as conn:
            conn.execute(text(query), records)
        logger.info("Carga finalizada: %s registros em '%s'.", len(records), table_name)
        return len(records)
    except SQLAlchemyError as e:
        logger.error("Erro ao carregar dados em '%s': %s", table_name, e)
        raise


def load_results(df: pd.DataFrame) -> int:
    """
    Carga específica para a tabela results, que necessita de lookups
    de chaves estrangeiras antes da inserção.
    """
    engine = get_sqlalchemy_engine()

    try:
        with engine.begin() as conn:
            race_map, driver_map, const_map = _get_lookups(conn)

            insert_query = text("""
                INSERT INTO results (race_id, driver_id, constructor_id, position, points)
                VALUES (:race_id, :driver_id, :constructor_id, :position, :points)
                ON CONFLICT (race_id, driver_id, constructor_id)
                DO UPDATE SET
                    position = EXCLUDED.position,
                    points = EXCLUDED.points;
            """)

            records = []
            for _, row in df.iterrows():
                r_id = race_map.get(row["race_name"])
                d_id = driver_map.get(row["driver_name"])
                c_id = const_map.get(row["constructor_name"])

                if r_id and d_id and c_id:
                    records.append({
                        "race_id": r_id,
                        "driver_id": d_id,
                        "constructor_id": c_id,
                        "position": row["position"],
                        "points": row["points"],
                    })
                else:
                    logger.warning(
                        "Lookup falhou para: race=%s, driver=%s, constructor=%s",
                        row["race_name"], row["driver_name"], row["constructor_name"],
                    )

            conn.execute(insert_query, records)
            logger.info("Carga de results finalizada: %s registros.", len(records))
            return len(records)
    except SQLAlchemyError as e:
        logger.error("Erro na carga de results: %s", e)
        raise
