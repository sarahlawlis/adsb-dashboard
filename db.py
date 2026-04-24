import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def _get_int_env(name, default):
    value = os.getenv(name)
    if value is None:
        return default

    try:
        return int(value)
    except ValueError:
        return default

def get_connection():
    connect_timeout = _get_int_env("DB_CONNECT_TIMEOUT", 5)
    statement_timeout_ms = _get_int_env("DB_STATEMENT_TIMEOUT_MS", 5000)

    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT", 5432),
        sslmode="require",
        connect_timeout=connect_timeout,
        options=f"-c statement_timeout={statement_timeout_ms}"
    )
