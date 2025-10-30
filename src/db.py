"""
db.py — Módulo auxiliar para conexión a PostgreSQL con SQLAlchemy.

Carga las credenciales desde un archivo .env (no subido al repo)
y ofrece funciones reutilizables para:
- Crear el engine de conexión
- Ejecutar consultas y devolver DataFrames
- Fijar el search_path (schema por defecto)
"""

from __future__ import annotations
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv


# --- Cargar variables de entorno ---
load_dotenv()


# --- Crear conexión (engine) ---
def get_engine() -> Engine:
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", "5432")
    db   = os.getenv("PG_DB")
    user = os.getenv("PG_USER")
    pwd  = os.getenv("PG_PASSWORD")
    ssl  = os.getenv("PG_SSLMODE", "require")

    # Validar que las credenciales existen
    if not all([host, db, user, pwd]):
        raise ValueError("❌ Faltan variables en .env (PG_HOST, PG_DB, PG_USER, PG_PASSWORD)")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}?sslmode={ssl}"
    engine = create_engine(url, future=True)
    return engine


# --- Ejecutar consulta y devolver DataFrame ---
def df_from_sql(sql: str, engine: Engine, params: dict | None = None) -> pd.DataFrame:
    """Ejecuta una consulta SQL y devuelve un DataFrame de pandas."""
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


# --- Cambiar el schema por defecto ---
def set_search_path(engine: Engine, schema: str) -> None:
    """Fija el esquema (search_path) en la sesión actual."""
    with engine.connect() as conn:
        conn.execute(text(f"SET search_path TO {schema};"))
        conn.commit()
