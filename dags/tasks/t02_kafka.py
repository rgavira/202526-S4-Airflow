"""
t02_kafka.py
------------
TAREA 02 — Publica cada fila del lote como mensaje JSON en Kafka.

Usa kafka-python (KafkaProducer) con confirmación acks='all'.
El delay entre mensajes (Variable DELAY_SECONDS) simula un flujo de streaming.

XCom IN:
    01_ingest_csv_batch.batch_data — JSON con los registros del lote
XCom OUT:
    published_count — número de mensajes publicados con éxito
"""

import json
import logging
import time

from airflow.models import Variable

from tasks.config import DEFAULT_DELAY_SECONDS, KAFKA_BOOTSTRAP, KAFKA_TOPIC


def publish_to_kafka(**context):
    from kafka import KafkaProducer
    from kafka.errors import KafkaError

    delay_sec = float(Variable.get("DELAY_SECONDS", default_var=DEFAULT_DELAY_SECONDS))
    ti = context["task_instance"]

    records = json.loads(ti.xcom_pull(task_ids="01_ingest_csv_batch", key="batch_data"))

    logging.info("=" * 60)
    logging.info("TAREA 02 — PUBLICAR EN KAFKA")
    logging.info(f"  Topic    : {KAFKA_TOPIC}")
    logging.info(f"  Broker   : {KAFKA_BOOTSTRAP}")
    logging.info(f"  Mensajes : {len(records)}")
    logging.info(f"  Delay    : {delay_sec}s por mensaje")
    logging.info("=" * 60)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",
        retries=3,
        request_timeout_ms=30_000,
    )

    published, errors = 0, 0

    for i, record in enumerate(records):
        try:
            key = record.get("timestamp", f"record_{i}")
            producer.send(KAFKA_TOPIC, key=key, value=record).get(timeout=15)
            published += 1
            if (i + 1) % 10 == 0:
                logging.info(f"  Progreso: {i + 1}/{len(records)} mensajes publicados...")
            if delay_sec > 0:
                time.sleep(delay_sec)
        except KafkaError as exc:
            logging.error(f"  Error en mensaje {i}: {exc}")
            errors += 1

    producer.flush()
    producer.close()

    logging.info(f"Publicación finalizada: {published} OK | {errors} errores")
    if errors:
        raise RuntimeError(f"Se produjeron {errors} errores al publicar en Kafka.")

    ti.xcom_push(key="published_count", value=published)
    return published
