"""Configurações centralizadas do projeto F1 Pipeline."""

import os

from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "f1db"),
    "user": os.getenv("DB_USER", "aug"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", "5432"),
}

SQLALCHEMY_DATABASE_URL = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

API_BASE_URL = "https://api.jolpi.ca/ergast/f1"
API_TIMEOUT = 30

DEFAULT_SEASON = 2023
