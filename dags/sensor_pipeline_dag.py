"""
sensor_pipeline_dag.py
======================
Entregable 3 — Pipeline de sensores del hogar
Máster en Ingeniería de Datos

Flujo:
  [01] Ingesta CSV por lotes        → tasks/t01_ingest.py
  [02] Publicar en Kafka            → tasks/t02_kafka.py
  [03] Registrar Kafka Connect→HDFS → tasks/t03_connector.py
  [04] Esperar datos en HDFS        → tasks/t04_sensor.py  (HttpSensor)
  [05] Transformar con Pandas       → tasks/t05_transform.py
  [06a] Crear tabla PostgreSQL      → DDL en tasks/t06_postgres.py
  [06b] Carga masiva a PostgreSQL   → tasks/t06_postgres.py
  [07/08] Analíticas + Reporte      → tasks/t07_report.py

Variables de Airflow configurables (UI → Admin → Variables):
  BATCH_SIZE    — filas por lote      (default: 50)
  DELAY_SECONDS — pausa entre mensajes Kafka, simula streaming (default: 0.1 s)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Importar funciones y constantes desde el paquete tasks/
from tasks.config import HDFS_TOPIC_PATH, POSTGRES_CONN_ID
from tasks.t01_ingest    import ingest_csv_batch
from tasks.t02_kafka     import publish_to_kafka
from tasks.t03_connector import register_hdfs_connector
from tasks.t04_sensor    import check_hdfs_has_data
from tasks.t05_transform import transform_and_normalize
from tasks.t06_postgres  import CREATE_TABLE_SQL, load_to_postgres
from tasks.t07_report    import run_analytics_and_report

# ─────────────────────────────────────────────────────────────────────────────
# Definición del DAG
# ─────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "entregable3",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="sensor_pipeline_dag",
    default_args=default_args,
    description="Pipeline: CSV → Kafka → HDFS → PostgreSQL → Analíticas",
    schedule_interval=None,   # Ejecución manual (cambia a '@hourly', etc. si quieres)
    catchup=False,
    tags=["entregable3", "kafka", "hdfs", "postgres"],
) as dag:

    # ── Tarea 01: Leer CSV y extraer lote ────────────────────────────────────
    t01 = PythonOperator(
        task_id="01_ingest_csv_batch",
        python_callable=ingest_csv_batch,
        provide_context=True,
    )

    # ── Tarea 02: Publicar lote en Kafka ──────────────────────────────────────
    t02 = PythonOperator(
        task_id="02_publish_to_kafka",
        python_callable=publish_to_kafka,
        provide_context=True,
    )

    # ── Tarea 03: Registrar/actualizar conector HDFS en Kafka Connect ─────────
    t03 = PythonOperator(
        task_id="03_register_hdfs_connector",
        python_callable=register_hdfs_connector,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(seconds=30),
    )

    # ── Tarea 04: Sensor — esperar que Kafka Connect descargue en HDFS ────────
    t04 = HttpSensor(
        task_id="04_wait_for_hdfs_data",
        http_conn_id="webhdfs_default",
        endpoint=f"/webhdfs/v1{HDFS_TOPIC_PATH}?op=LISTSTATUS",
        response_check=check_hdfs_has_data,
        extra_options={"check_response": False},  # No falla en 404
        poke_interval=30,       # Comprueba cada 30 s
        timeout=600,            # Timeout máximo: 10 min
        mode="reschedule",      # Libera el worker mientras espera
    )

    # ── Tarea 05: Transformar y normalizar datos con Pandas ───────────────────
    t05 = PythonOperator(
        task_id="05_transform_normalize",
        python_callable=transform_and_normalize,
        provide_context=True,
    )

    # ── Tarea 06a: Crear tabla en PostgreSQL (idempotente) ────────────────────
    t06a = PostgresOperator(
        task_id="06a_create_postgres_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=CREATE_TABLE_SQL,
    )

    # ── Tarea 06b: Carga masiva a PostgreSQL ──────────────────────────────────
    t06b = PythonOperator(
        task_id="06b_load_to_postgres",
        python_callable=load_to_postgres,
        provide_context=True,
    )

    # ── Tarea 07/08: Analíticas y generación de reporte ───────────────────────
    t07 = PythonOperator(
        task_id="07_08_analytics_and_report",
        python_callable=run_analytics_and_report,
        provide_context=True,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Grafo de dependencias
    # ─────────────────────────────────────────────────────────────────────────
    t01 >> t02 >> t03 >> t04 >> t05 >> t06a >> t06b >> t07
