"""
t05_transform.py
----------------
TAREA 05 — Transformación y normalización de datos con Pandas (sin Spark).

Pasos:
  1. Lista recursivamente los ficheros JSON en HDFS via WebHDFS REST API.
  2. Lee y concatena todos los registros en un DataFrame.
  3. Transformaciones de negocio:
       a) Conversión y validación de tipos
       b) Eliminación de registros con timestamp nulo
       c) Tratamiento de outliers (clipado IQR × 3) en temperaturas
       d) Imputación de nulos con la media de la columna
       e) Cálculo de métricas agregadas interiores:
            avg_indoor_temperature  — media salón + habitación + despacho
            avg_indoor_humidity     — ídem para humedad
            avg_indoor_air_quality  — ídem para calidad del aire
  4. Guarda el CSV resultante en /tmp/ para la siguiente tarea.

XCom OUT:
    transformed_path — ruta del CSV resultante
    row_count        — número de filas en el DataFrame final
"""

import json
import logging

import pandas as pd
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

from tasks.config import HDFS_TOPIC_PATH, PG_TABLE, POSTGRES_CONN_ID, TRANSFORMED_TMP, WEBHDFS_BASE
from tasks.log_utils import setup_file_logging


# ── Watermark ────────────────────────────────────────────────────────────────

def _get_pg_watermark():
    """Devuelve el MAX(timestamp) ya cargado en PostgreSQL, o None si la tabla está vacía."""
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        result = hook.get_first(f"SELECT MAX(timestamp) FROM {PG_TABLE}")
        return result[0] if result and result[0] is not None else None
    except Exception:
        return None  # Tabla aún no existe (primera ejecución)


# ── Helpers WebHDFS ───────────────────────────────────────────────────────────

def _list_hdfs_recursive(path: str) -> list[str]:
    """Devuelve las rutas de todos los ficheros JSON bajo `path` (recursivo)."""
    url = f"{WEBHDFS_BASE}/webhdfs/v1{path}?op=LISTSTATUS"
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        logging.warning(f"No se pudo listar {path}: HTTP {resp.status_code}")
        return []
    found = []
    for item in resp.json().get("FileStatuses", {}).get("FileStatus", []):
        child = f"{path}/{item['pathSuffix']}"
        if item["type"] == "DIRECTORY":
            found.extend(_list_hdfs_recursive(child))
        elif item["type"] == "FILE" and item["pathSuffix"].endswith(".json"):
            found.append(child)
    return found


def _read_hdfs_file(hdfs_path: str) -> str:
    """Lee un fichero de HDFS siguiendo el redirect automático al Datanode."""
    url = f"{WEBHDFS_BASE}/webhdfs/v1{hdfs_path}?op=OPEN"
    resp = requests.get(url, allow_redirects=True, timeout=60)
    resp.raise_for_status()
    return resp.text


# ── Función principal ─────────────────────────────────────────────────────────

def transform_and_normalize(**context):
    setup_file_logging("t05_transform", context["execution_date"])
    SEP = "=" * 60
    logging.info(SEP)
    logging.info("TAREA 05 — TRANSFORMACIÓN Y NORMALIZACIÓN (Pandas)")
    logging.info(f"  Ruta HDFS : {HDFS_TOPIC_PATH}")
    logging.info(SEP)

    # ── FASE 1: Descubrir y leer ficheros HDFS ────────────────────────────────
    logging.info("[FASE 1/4] Listando ficheros JSON en HDFS...")
    json_files = _list_hdfs_recursive(HDFS_TOPIC_PATH)
    if not json_files:
        raise ValueError(f"No se encontraron ficheros JSON en HDFS: {HDFS_TOPIC_PATH}")
    logging.info(f"  Ficheros encontrados : {len(json_files)}")
    for f in json_files:
        logging.info(f"    · {f}")

    logging.info("[FASE 1/4] Leyendo registros...")
    all_records = []
    lines_invalid = 0
    for hdfs_file in json_files:
        for line in _read_hdfs_file(hdfs_file).strip().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                if "payload" in record and isinstance(record["payload"], dict):
                    record = record["payload"]
                all_records.append(record)
            except json.JSONDecodeError as exc:
                lines_invalid += 1
                logging.warning(f"  Línea inválida omitida: {exc}")

    n_hdfs = len(all_records)
    logging.info(f"  Registros leídos de HDFS   : {n_hdfs}")
    if lines_invalid:
        logging.info(f"  Líneas JSON inválidas       : {lines_invalid} (descartadas)")
    df = pd.DataFrame(all_records)

    # ── FASE 2: Limpieza y tipos ──────────────────────────────────────────────
    logging.info("[FASE 2/4] Limpieza y validación de tipos...")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    numeric_cols = [
        "temperature_salon",    "humidity_salon",    "air_salon",
        "temperature_chambre",  "humidity_chambre",  "air_chambre",
        "temperature_bureau",   "humidity_bureau",   "air_bureau",
        "temperature_exterieur","humidity_exterieur","air_exterieur",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 2b. Eliminar registros sin timestamp
    n_before_ts = len(df)
    df = df.dropna(subset=["timestamp"])
    n_dropped_ts = n_before_ts - len(df)
    logging.info(f"  Eliminados por timestamp nulo : {n_dropped_ts}  ({n_before_ts} → {len(df)})")

    # 2b2. Watermark incremental — solo procesar registros nuevos
    watermark = _get_pg_watermark()
    if watermark is not None:
        n_before_wm = len(df)
        df = df[df["timestamp"] > pd.Timestamp(watermark)]
        n_dropped_wm = n_before_wm - len(df)
        logging.info(f"  Watermark PG                  : {watermark}")
        logging.info(f"  Filtrados por watermark        : -{n_dropped_wm}  ({n_before_wm} → {len(df)})")
    else:
        n_dropped_wm = 0
        logging.info("  Watermark PG                  : sin datos previos → procesando todo")

    if df.empty:
        logging.warning("  No hay registros nuevos tras el watermark. Nada que cargar.")
        df.to_csv(TRANSFORMED_TMP, index=False)
        ti = context["task_instance"]
        ti.xcom_push(key="transformed_path", value=TRANSFORMED_TMP)
        ti.xcom_push(key="row_count",        value=0)
        ti.xcom_push(key="n_hdfs_read",      value=n_hdfs)
        ti.xcom_push(key="n_dropped_ts",     value=n_dropped_ts)
        ti.xcom_push(key="n_dropped_dedup",  value=0)
        ti.xcom_push(key="n_dropped_wm",     value=n_dropped_wm)
        return 0

    # 2c. Outliers: límites físicos absolutos (no estadísticos)
    #   Temperatura: sensor roto si marca < -40 °C o > 80 °C
    #   Humedad    : imposible fuera de [0, 100] %
    #   Aire (CO₂) : sensor roto si marca < 0 o > 10000 ppm
    PHYSICAL_BOUNDS = {
        "temperature": (-40.0, 80.0),
        "humidity":    (  0.0, 100.0),
        "air":         (  0.0, 10000.0),
    }
    n_outlier_cols = 0
    for col in [c for c in numeric_cols if c in df.columns]:
        key = next((k for k in PHYSICAL_BOUNDS if col.startswith(k)), None)
        if key is None:
            continue
        lo, hi = PHYSICAL_BOUNDS[key]
        n_out = int(((df[col] < lo) | (df[col] > hi)).sum())
        if n_out:
            df[col] = df[col].clip(lo, hi)
            logging.info(f"  Outliers físicos en '{col}': {n_out} valores fuera de [{lo}, {hi}]")
            n_outlier_cols += n_out
    if n_outlier_cols == 0:
        logging.info("  Outliers físicos            : 0 (todos los valores dentro de límites)")

    # 2d. Imputación de nulos con la media
    n_imputed_total = 0
    for col in numeric_cols:
        if col in df.columns and (n_null := int(df[col].isnull().sum())):
            fill = df[col].mean()
            df[col] = df[col].fillna(fill)
            logging.info(f"  Nulos imputados en '{col}': {n_null} → media={fill:.2f}")
            n_imputed_total += n_null
    if n_imputed_total == 0:
        logging.info("  Nulos imputados             : 0 (ningún nulo numérico)")

    # ── FASE 3: Métricas agregadas + deduplicación ────────────────────────────
    logging.info("[FASE 3/4] Calculando métricas interiores y deduplicando...")
    t_cols = [c for c in ["temperature_salon", "temperature_chambre", "temperature_bureau"] if c in df.columns]
    h_cols = [c for c in ["humidity_salon",    "humidity_chambre",    "humidity_bureau"]    if c in df.columns]
    a_cols = [c for c in ["air_salon",         "air_chambre",         "air_bureau"]         if c in df.columns]

    df["avg_indoor_temperature"] = df[t_cols].mean(axis=1).round(2)
    df["avg_indoor_humidity"]    = df[h_cols].mean(axis=1).round(2)
    df["avg_indoor_air_quality"] = df[a_cols].mean(axis=1).round(2)

    n_before_dedup = len(df)
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    n_dropped_dedup = n_before_dedup - len(df)
    logging.info(f"  Duplicados por timestamp    : {n_dropped_dedup}  ({n_before_dedup} → {len(df)})")

    # ── FASE 4: Persistir y resumen ───────────────────────────────────────────
    logging.info("[FASE 4/4] Guardando CSV y generando resumen...")
    df.to_csv(TRANSFORMED_TMP, index=False)

    logging.info(SEP)
    logging.info("  FUNNEL DE DATOS — T05")
    logging.info(f"  Leídos de HDFS              : {n_hdfs}")
    logging.info(f"  - Timestamp nulo            : -{n_dropped_ts}")
    logging.info(f"  - Ya en PG (watermark)      : -{n_dropped_wm}")
    logging.info(f"  - Duplicados (timestamp)    : -{n_dropped_dedup}")
    logging.info(f"  = REGISTROS NUEVOS          : {len(df)}")
    logging.info(f"  Nulos imputados (numérico)  : {n_imputed_total}")
    logging.info(f"  Outliers clipados           : {n_outlier_cols}")
    logging.info(f"  CSV guardado en             : {TRANSFORMED_TMP}")
    logging.info(SEP)

    ti = context["task_instance"]
    ti.xcom_push(key="transformed_path", value=TRANSFORMED_TMP)
    ti.xcom_push(key="row_count",        value=len(df))
    ti.xcom_push(key="n_hdfs_read",      value=n_hdfs)
    ti.xcom_push(key="n_dropped_ts",     value=n_dropped_ts)
    ti.xcom_push(key="n_dropped_wm",     value=n_dropped_wm)
    ti.xcom_push(key="n_dropped_dedup",  value=n_dropped_dedup)
    return len(df)
