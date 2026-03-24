"""
t04_sensor.py
-------------
TAREA 04 — Función de verificación para el HttpSensor de HDFS.

El HttpSensor llama a esta función con el objeto Response de cada poke.
Devuelve True cuando el directorio HDFS del topic existe Y contiene elementos
(subdirectorios de partición creados por Kafka Connect).

Maneja explícitamente el caso 404 (directorio aún no creado) para que el
sensor no falle sino que siga esperando.
"""

import logging


def check_hdfs_has_data(response) -> bool:
    """
    Función response_check para HttpSensor apuntando a WebHDFS ListStatus.

    Args:
        response: objeto requests.Response devuelto por el HttpSensor.

    Returns:
        True  — directorio existe y tiene contenido (continúa el pipeline).
        False — directorio vacío o no existe (el sensor vuelve a polear).
    """
    if response.status_code == 404:
        logging.info("  [Sensor] Directorio HDFS aún no existe. Esperando...")
        return False

    if response.status_code != 200:
        logging.warning(f"  [Sensor] Respuesta inesperada: HTTP {response.status_code}. Reintentando...")
        return False

    try:
        file_statuses = (
            response.json()
            .get("FileStatuses", {})
            .get("FileStatus", [])
        )
        if file_statuses:
            logging.info(f"  [Sensor] {len(file_statuses)} elemento(s) en HDFS. Pipeline continúa.")
            return True

        logging.info("  [Sensor] Directorio existe pero está vacío. Esperando que Kafka Connect descargue...")
        return False

    except Exception as exc:
        logging.warning(f"  [Sensor] Error al parsear respuesta WebHDFS: {exc}")
        return False
