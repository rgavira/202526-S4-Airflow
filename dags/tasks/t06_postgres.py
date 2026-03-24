"""
t06_postgres.py
---------------
TAREA 06 — Modelado y carga masiva en PostgreSQL.

Contiene:
  CREATE_TABLE_SQL — DDL de la tabla home_sensor_readings (para PostgresOperator)
  load_to_postgres — carga masiva con psycopg2.extras.execute_values

La restricción UNIQUE (timestamp) protege contra duplicados en recargas.
ON CONFLICT DO UPDATE garantiza idempotencia.

XCom IN:
    05_transform_normalize.transformed_path — ruta del CSV transformado
XCom OUT:
    rows_loaded — número de filas insertadas/actualizadas
"""

import logging

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from tasks.config import PG_TABLE, POSTGRES_CONN_ID
from tasks.log_utils import setup_file_logging

# ── DDL ───────────────────────────────────────────────────────────────────────
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {PG_TABLE} (
    id                      SERIAL      PRIMARY KEY,
    timestamp               TIMESTAMP   NOT NULL,

    -- Salón (salon)
    temperature_salon       FLOAT,
    humidity_salon          FLOAT,
    air_salon               FLOAT,

    -- Habitación (chambre)
    temperature_chambre     FLOAT,
    humidity_chambre        FLOAT,
    air_chambre             FLOAT,

    -- Despacho (bureau)
    temperature_bureau      FLOAT,
    humidity_bureau         FLOAT,
    air_bureau              FLOAT,

    -- Exterior
    temperature_exterieur   FLOAT,
    humidity_exterieur      FLOAT,
    air_exterieur           FLOAT,

    -- Métricas calculadas
    avg_indoor_temperature  FLOAT,
    avg_indoor_humidity     FLOAT,
    avg_indoor_air_quality  FLOAT,

    -- Metadatos de carga
    loaded_at               TIMESTAMP   DEFAULT NOW(),
    pipeline_run_id         TEXT,

    CONSTRAINT uq_sensor_timestamp UNIQUE (timestamp)
);

CREATE INDEX IF NOT EXISTS idx_{PG_TABLE}_timestamp
    ON {PG_TABLE} (timestamp);

CREATE INDEX IF NOT EXISTS idx_{PG_TABLE}_ext_temp
    ON {PG_TABLE} (temperature_exterieur);

CREATE INDEX IF NOT EXISTS idx_{PG_TABLE}_air_quality
    ON {PG_TABLE} (avg_indoor_air_quality);
"""

# ── Carga masiva ──────────────────────────────────────────────────────────────
_COL_ORDER = [
    "timestamp",
    "temperature_salon",    "humidity_salon",    "air_salon",
    "temperature_chambre",  "humidity_chambre",  "air_chambre",
    "temperature_bureau",   "humidity_bureau",   "air_bureau",
    "temperature_exterieur","humidity_exterieur","air_exterieur",
    "avg_indoor_temperature","avg_indoor_humidity","avg_indoor_air_quality",
]


def load_to_postgres(**context):
    setup_file_logging("t06b_postgres", context["execution_date"])
    from psycopg2.extras import execute_values

    ti = context["task_instance"]
    path   = ti.xcom_pull(task_ids="05_transform_normalize", key="transformed_path")
    run_id = context["run_id"]

    logging.info("=" * 60)
    logging.info("TAREA 06b — CARGA MASIVA EN POSTGRESQL")
    logging.info(f"  Tabla  : {PG_TABLE}")
    logging.info(f"  Origen : {path}")
    logging.info("=" * 60)

    df = pd.read_csv(path)
    available = [c for c in _COL_ORDER if c in df.columns]
    df_load   = df[available].where(pd.notnull(df[available]), None)

    rows      = [tuple(row) + (run_id,) for row in df_load.itertuples(index=False, name=None)]
    col_names = available + ["pipeline_run_id"]
    update_set = ", ".join(
        [f"{c} = EXCLUDED.{c}" for c in available if c != "timestamp"] + ["loaded_at = NOW()"]
    )

    sql = f"""
        INSERT INTO {PG_TABLE} ({', '.join(col_names)})
        VALUES %s
        ON CONFLICT (timestamp) DO UPDATE SET {update_set};
    """

    hook   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn   = hook.get_conn()
    cursor = conn.cursor()
    try:
        execute_values(cursor, sql, rows, page_size=100)
        conn.commit()
        logging.info(f"Carga completada: {len(rows)} filas insertadas/actualizadas.")
    except Exception as exc:
        conn.rollback()
        raise RuntimeError(f"Error en la carga masiva: {exc}") from exc
    finally:
        cursor.close()
        conn.close()

    ti.xcom_push(key="rows_loaded", value=len(rows))
