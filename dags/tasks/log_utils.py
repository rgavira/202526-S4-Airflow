"""
log_utils.py
------------
Helper para duplicar los logs de cada tarea a fichero.

Uso en cada tarea:
    from tasks.log_utils import setup_file_logging
    ...
    def mi_tarea(**context):
        log_path = setup_file_logging("t01_ingest", context["execution_date"])
        ...

Estructura resultante en logs_analytics/:
    tasks/
        20260324_120500/
            t01_ingest.log
            t02_kafka.log
            ...
"""

import logging
import os

from tasks.config import LOGS_DIR


def setup_file_logging(task_name: str, execution_date) -> str:
    """
    Añade un FileHandler al logger raíz para que todos los logging.info/warning/error
    del proceso de esta tarea se escriban también en disco.

    Devuelve la ruta del fichero de log creado.
    """
    run_label = (
        execution_date.strftime("%Y%m%d_%H%M%S")
        if hasattr(execution_date, "strftime")
        else str(execution_date)[:19].replace(":", "").replace("-", "").replace(" ", "_").replace("T", "_")
    )

    task_logs_dir = os.path.join(LOGS_DIR, "tasks", run_label)
    os.makedirs(task_logs_dir, exist_ok=True)

    log_path = os.path.join(task_logs_dir, f"{task_name}.log")

    handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s"))
    handler.setLevel(logging.DEBUG)

    logging.getLogger().addHandler(handler)

    return log_path
