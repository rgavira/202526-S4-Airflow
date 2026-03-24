"""
t03_connector.py
----------------
TAREA 03 — Registra el HDFS3 Sink Connector en Kafka Connect (idempotente).

Lógica:
  • Conector NO existe → POST /connectors          (crear)
  • Conector YA existe → PUT  /connectors/{n}/config (actualizar)

El conector lee del topic 'home_sensors' y escribe ficheros JSON en HDFS bajo:
  /user/kafka/home_sensors/partition=0/home_sensors+0+XXXXXXXX+YYYYYYYY.json

Parámetros clave:
  flush.size            = 50   → vuelca cada 50 mensajes
  rotate.interval.ms    = 30000 → o cada 30 segundos (lo primero que ocurra)
  hdfs.authentication.kerberos = false → sin Kerberos (entorno de desarrollo)
"""

import json
import logging
import time

import requests

from tasks.config import (
    CONNECTOR_NAME,
    HDFS_TOPICS_DIR,
    KAFKA_CONNECT_URL,
    KAFKA_TOPIC,
)

_CONNECTOR_CONFIG = {
    "connector.class": "io.confluent.connect.hdfs3.Hdfs3SinkConnector",
    "tasks.max": "1",
    "topics": KAFKA_TOPIC,
    "hdfs.url": "hdfs://namenode:9000",
    "topics.dir": HDFS_TOPICS_DIR,
    "storage.class": "io.confluent.connect.hdfs3.storage.HdfsStorage",
    "format.class": "io.confluent.connect.hdfs3.json.JsonFormat",
    "flush.size": "50",
    "rotate.interval.ms": "30000",
    "rotate.schedule.interval.ms": "60000",
    "schema.compatibility": "NONE",
    "locale": "en_US",
    "timezone": "UTC",
    "hdfs.authentication.kerberos": "false",
}


def register_hdfs_connector(**context):
    headers    = {"Content-Type": "application/json"}
    check_url  = f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}"
    config_url = f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/config"
    create_url = f"{KAFKA_CONNECT_URL}/connectors"

    logging.info("=" * 60)
    logging.info("TAREA 03 — REGISTRAR KAFKA CONNECT → HDFS (idempotente)")
    logging.info(f"  Endpoint : {KAFKA_CONNECT_URL}")
    logging.info(f"  Conector : {CONNECTOR_NAME}")
    logging.info("=" * 60)

    resp = requests.get(check_url, timeout=30)

    if resp.status_code == 200:
        logging.info("Conector existente → actualizando configuración (PUT)...")
        r = requests.put(config_url, json=_CONNECTOR_CONFIG, headers=headers, timeout=30)
        r.raise_for_status()
        logging.info(f"Conector actualizado. HTTP {r.status_code}")

    elif resp.status_code == 404:
        logging.info("Conector no existe → creando (POST)...")
        r = requests.post(
            create_url,
            json={"name": CONNECTOR_NAME, "config": _CONNECTOR_CONFIG},
            headers=headers,
            timeout=30,
        )
        r.raise_for_status()
        logging.info(f"Conector creado. HTTP {r.status_code}")
        logging.info(f"Respuesta: {json.dumps(r.json(), indent=2)}")

    else:
        raise RuntimeError(
            f"Error inesperado al comprobar el conector: HTTP {resp.status_code}\n{resp.text}"
        )

    # Verificar estado
    time.sleep(5)
    status = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=30)
    if status.status_code == 200:
        state = status.json().get("connector", {}).get("state", "UNKNOWN")
        logging.info(f"Estado del conector: {state}")
        if state == "FAILED":
            raise RuntimeError(f"El conector entró en estado FAILED: {status.json()}")
