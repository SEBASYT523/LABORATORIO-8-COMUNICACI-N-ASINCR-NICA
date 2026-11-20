"""
ACTIVIDAD 5 - Acceso a la base de datos PostgreSQL (Neon o local).

La cadena de conexion se lee de la variable de entorno DATABASE_URL, para que
la contrasena nunca quede en el codigo ni en el repositorio.

  Neon:   postgresql://usuario:clave@ep-xxxx.us-east-2.aws.neon.tech/neondb?sslmode=require
  Local:  postgresql://escrow:escrow@localhost:5432/escrow   (docker compose)
"""
import os
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg

URL_LOCAL = "postgresql://escrow:escrow@localhost:5432/escrow"

ESQUEMA = """
CREATE TABLE IF NOT EXISTS transacciones (
    id           TEXT PRIMARY KEY,
    comprador    TEXT        NOT NULL,
    vendedor     TEXT        NOT NULL,
    valor        BIGINT      NOT NULL CHECK (valor > 0),
    estado       TEXT        NOT NULL,
    creado       TIMESTAMPTZ NOT NULL DEFAULT now(),
    actualizado  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS eventos_procesados (
    evento_id      TEXT PRIMARY KEY,           -- la llave primaria garantiza la idempotencia
    transaccion_id TEXT        NOT NULL,
    tipo           TEXT        NOT NULL,
    procesado      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS historial (
    id              BIGSERIAL PRIMARY KEY,
    transaccion_id  TEXT        NOT NULL REFERENCES transacciones(id),
    estado_anterior TEXT        NOT NULL,
    estado_nuevo    TEXT        NOT NULL,
    evento_id       TEXT        NOT NULL,
    momento         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS idx_historial_tx ON historial(transaccion_id);
CREATE INDEX IF NOT EXISTS idx_tx_estado ON transacciones(estado);
"""


def url_bd():
    url = os.getenv("DATABASE_URL", URL_LOCAL)
    # Neon agrega 'channel_binding=require', que asyncpg no reconoce: se quita.
    partes = urlsplit(url)
    query = [(k, v) for k, v in parse_qsl(partes.query) if k != "channel_binding"]
    return urlunsplit(partes._replace(query=urlencode(query)))


async def crear_pool():
    # statement_cache_size=0: compatible con el pooler de Neon (PgBouncer)
    pool = await asyncpg.create_pool(url_bd(), min_size=1, max_size=5, statement_cache_size=0)
    async with pool.acquire() as con:
        await con.execute(ESQUEMA)
    return pool


def destino():
    """Texto para el log: a que servidor nos conectamos (sin la contrasena)."""
    p = urlsplit(url_bd())
    return f"{p.hostname}/{p.path.lstrip('/')}"
