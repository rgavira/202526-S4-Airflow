"""
t01_ingest.py
-------------
TAREA 01 — Lee el CSV de sensores y extrae un lote configurable de filas.

Simula la llegada de datos por streaming/lotes pequeños.
El tamaño del lote se controla con la Variable de Airflow BATCH_SIZE.

XCom OUT:
    batch_data  — JSON con los registros del lote (lista de dicts)
    batch_size  — número de filas efectivamente leídas
"""

import json
import logging

import pandas as pd
from airflow.models import Variable

from tasks.config import CSV_PATH, DEFAULT_BATCH_SIZE


def ingest_csv_batch(**context):
    batch_size = int(Variable.get("BATCH_SIZE", default_var=DEFAULT_BATCH_SIZE))

    logging.info("=" * 60)
    logging.info("TAREA 01 — INGESTA DEL CSV")
    logging.info(f"  Archivo : {CSV_PATH}")
    logging.info(f"  Lote    : primeras {batch_size} filas")
    logging.info("=" * 60)

    df = pd.read_csv(CSV_PATH)
    logging.info(f"Dataset completo: {len(df)} registros | columnas: {list(df.columns)}")

    batch = df.head(batch_size).copy()
    logging.info(f"Lote extraído   : {len(batch)} filas")
    logging.info(f"Rango temporal  : {batch['timestamp'].min()}  →  {batch['timestamp'].max()}")

    ti = context["task_instance"]
    ti.xcom_push(key="batch_data", value=batch.to_json(orient="records", date_format="iso"))
    ti.xcom_push(key="batch_size", value=len(batch))

    logging.info("Lote enviado a XCom.")
    return len(batch)
