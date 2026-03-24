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

from tasks.config import HDFS_TOPIC_PATH, TRANSFORMED_TMP, WEBHDFS_BASE


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
    logging.info("=" * 60)
    logging.info("TAREA 05 — TRANSFORMACIÓN Y NORMALIZACIÓN (Pandas)")
    logging.info(f"  Ruta HDFS: {HDFS_TOPIC_PATH}")
    logging.info("=" * 60)

    # 1. Descubrir ficheros
    json_files = _list_hdfs_recursive(HDFS_TOPIC_PATH)
    if not json_files:
        raise ValueError(f"No se encontraron ficheros JSON en HDFS: {HDFS_TOPIC_PATH}")
    logging.info(f"Ficheros JSON encontrados: {len(json_files)}")

    # 2. Leer y concatenar
    all_records = []
    for hdfs_file in json_files:
        logging.info(f"  Leyendo: {hdfs_file}")
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
                logging.warning(f"  Línea inválida omitida: {exc}")

    logging.info(f"Total registros leídos desde HDFS: {len(all_records)}")
    df = pd.DataFrame(all_records)

    # 3a. Tipos de datos
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

    # 3b. Eliminar registros sin timestamp
    n_before = len(df)
    df = df.dropna(subset=["timestamp"])
    if (dropped := n_before - len(df)):
        logging.info(f"Registros eliminados por timestamp nulo: {dropped}")

    # 3c. Outliers: clipado IQR × 3 en columnas de temperatura
    for col in [c for c in numeric_cols if "temperature" in c and c in df.columns]:
        q1, q3 = df[col].quantile(0.25), df[col].quantile(0.75)
        lo, hi = q1 - 3 * (q3 - q1), q3 + 3 * (q3 - q1)
        n_out = ((df[col] < lo) | (df[col] > hi)).sum()
        if n_out:
            df[col] = df[col].clip(lo, hi)
            logging.info(f"  Outliers clipados en '{col}': {n_out} valores → [{lo:.1f}, {hi:.1f}]")

    # 3d. Imputación de nulos con la media
    for col in numeric_cols:
        if col in df.columns and (n_null := df[col].isnull().sum()):
            fill = df[col].mean()
            df[col] = df[col].fillna(fill)
            logging.info(f"  Nulos imputados en '{col}': {n_null} → media={fill:.2f}")

    # 3e. Métricas agregadas interiores
    t_cols = [c for c in ["temperature_salon", "temperature_chambre", "temperature_bureau"] if c in df.columns]
    h_cols = [c for c in ["humidity_salon",    "humidity_chambre",    "humidity_bureau"]    if c in df.columns]
    a_cols = [c for c in ["air_salon",         "air_chambre",         "air_bureau"]         if c in df.columns]

    df["avg_indoor_temperature"] = df[t_cols].mean(axis=1).round(2)
    df["avg_indoor_humidity"]    = df[h_cols].mean(axis=1).round(2)
    df["avg_indoor_air_quality"] = df[a_cols].mean(axis=1).round(2)

    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    logging.info(f"Transformación completada. Shape final: {df.shape}")
    logging.info(
        f"\n{df[['avg_indoor_temperature','avg_indoor_humidity','temperature_exterieur']].describe()}"
    )

    # 4. Persistir CSV temporal
    df.to_csv(TRANSFORMED_TMP, index=False)
    logging.info(f"CSV transformado guardado en: {TRANSFORMED_TMP}")

    ti = context["task_instance"]
    ti.xcom_push(key="transformed_path", value=TRANSFORMED_TMP)
    ti.xcom_push(key="row_count",        value=len(df))
    return len(df)
