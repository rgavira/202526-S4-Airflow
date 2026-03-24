"""
config.py
---------
Constantes globales del pipeline.
Modifica aquí si cambias la infraestructura (hosts, puertos, rutas).
"""

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_TOPIC       = "home_sensors"
KAFKA_BOOTSTRAP   = "kafka:29092"

# ── Kafka Connect ─────────────────────────────────────────────────────────────
KAFKA_CONNECT_URL = "http://kafka-connect:8083"
CONNECTOR_NAME    = "hdfs3-sink-home-sensors"

# ── HDFS / WebHDFS ────────────────────────────────────────────────────────────
WEBHDFS_BASE      = "http://namenode:9870"
HDFS_TOPICS_DIR   = "/user/kafka"           # topics.dir del conector Kafka Connect
HDFS_TOPIC_PATH   = f"{HDFS_TOPICS_DIR}/{KAFKA_TOPIC}"

# ── Airflow / ficheros ────────────────────────────────────────────────────────
CSV_PATH          = "/opt/airflow/data/home_temperature_and_humidity_smoothed_filled.csv"
TRANSFORMED_TMP   = "/tmp/sensor_transformed.csv"
LOGS_DIR          = "/opt/airflow/logs_analytics"

# ── PostgreSQL ────────────────────────────────────────────────────────────────
POSTGRES_CONN_ID  = "postgres_default"
PG_TABLE          = "home_sensor_readings"

# ── Valores por defecto para Variables de Airflow ─────────────────────────────
DEFAULT_BATCH_SIZE    = 50
DEFAULT_DELAY_SECONDS = 0.1
