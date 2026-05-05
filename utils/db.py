"""Gerenciamento centralizado de conexões com o banco de dados."""

import psycopg2
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from config.settings import DB_CONFIG, SQLALCHEMY_DATABASE_URL
from utils.logger import get_logger

logger = get_logger(__name__)


def get_psycopg2_connection() -> connection:
    """Retorna uma conexão psycopg2 branca."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Conexão psycopg2 estabelecida com sucesso.")
        return conn
    except psycopg2.Error as e:
        logger.error("Falha ao conectar ao banco via psycopg2: %s", e)
        raise


def get_sqlalchemy_engine() -> Engine:
    """Retorna uma engine SQLAlchemy para uso com pandas."""
    try:
        engine = create_engine(SQLALCHEMY_DATABASE_URL)
        logger.info("Engine SQLAlchemy criada com sucesso.")
        return engine
    except Exception as e:
        logger.error("Falha ao criar engine SQLAlchemy: %s", e)
        raise
