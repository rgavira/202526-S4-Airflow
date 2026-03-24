"""
t01_ingest.py
-------------
TAREA 01 — Lee el CSV de sensores y extrae el siguiente lote pendiente.

Usa dos Variables de Airflow para simular streaming por ventanas:
  BATCH_SIZE   — filas por lote (default: 50)
  BATCH_OFFSET — fila desde la que empieza este lote (se actualiza al terminar)

Comportamiento:
  - Cada ejecución del DAG avanza BATCH_SIZE filas en el CSV.
  - Al llegar al final del fichero, el offset vuelve a 0 (modo circular).
  - Si el lote final tiene menos filas que BATCH_SIZE, se usa lo que queda.

Para procesar el CSV completo de forma continua, programa el DAG con
schedule_interval (p.ej. '@hourly') en lugar de ejecutarlo manualmente.

XCom OUT:
    batch_data   — JSON con los registros del lote (lista de dicts)
    batch_size   — número de filas efectivamente leídas
    batch_offset — offset de inicio de este lote (para trazabilidad)
"""

import logging

import pandas as pd
from airflow.models import Variable

from tasks.config import CSV_PATH, DEFAULT_BATCH_SIZE
from tasks.log_utils import setup_file_logging


def ingest_csv_batch(**context):
    setup_file_logging("t01_ingest", context["execution_date"])
    batch_size = int(Variable.get("BATCH_SIZE",   default_var=DEFAULT_BATCH_SIZE))
    offset     = int(Variable.get("BATCH_OFFSET", default_var=0))

    logging.info("=" * 60)
    logging.info("TAREA 01 — INGESTA DEL CSV")
    logging.info(f"  Archivo : {CSV_PATH}")
    logging.info(f"  Offset  : {offset}  |  Lote: {batch_size} filas")
    logging.info("=" * 60)

    df = pd.read_csv(CSV_PATH)
    total = len(df)
    logging.info(f"Dataset completo: {total} registros")

    # Extraer el lote desde el offset actual
    batch = df.iloc[offset: offset + batch_size].copy()

    if batch.empty:
        # Offset fuera de rango — reiniciar y tomar desde el principio
        logging.warning("Offset fuera de rango. Reiniciando a 0.")
        offset = 0
        batch  = df.iloc[0:batch_size].copy()

    # Calcular el siguiente offset (circular)
    next_offset = (offset + len(batch)) % total
    Variable.set("BATCH_OFFSET", next_offset)

    logging.info(f"Lote extraído   : {len(batch)} filas  (filas {offset}–{offset + len(batch) - 1})")
    logging.info(f"Rango temporal  : {batch['timestamp'].min()}  →  {batch['timestamp'].max()}")
    logging.info(f"Próximo offset  : {next_offset}  (de {total} filas totales)")

    ti = context["task_instance"]
    ti.xcom_push(key="batch_data",   value=batch.to_json(orient="records", date_format="iso"))
    ti.xcom_push(key="batch_size",   value=len(batch))
    ti.xcom_push(key="batch_offset", value=offset)

    return len(batch)
