#!/bin/bash
# =============================================================================
# init-airflow.sh — Script de inicialización del contenedor Airflow
#
# Realiza en orden:
#   1. Inicializa / migra la base de datos de Airflow
#   2. Crea el usuario administrador
#   3. Registra las conexiones necesarias (Kafka, Postgres, WebHDFS, KafkaConnect)
#   4. Fija las Variables de Airflow (BATCH_SIZE, DELAY_SECONDS)
#   5. Arranca el scheduler en background y el webserver en foreground
# =============================================================================
set -e

echo "======================================================="
echo " Entregable 3 — Inicializando Apache Airflow 2.9.3"
echo "======================================================="

echo "[1/5] Esperando a que PostgreSQL esté disponible..."
sleep 10

echo "[2/5] Inicializando / migrando base de datos de Airflow..."
airflow db upgrade 2>/dev/null || airflow db init

echo "[3/5] Creando usuario administrador (admin/admin)..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@entregable3.local || echo "   → Usuario 'admin' ya existe, continuando."

echo "[4/5] Registrando conexiones de Airflow..."

# ── Kafka Broker ─────────────────────────────────────────────────────────────
airflow connections add kafka_default \
    --conn-type kafka \
    --conn-host kafka \
    --conn-port 29092 \
    --conn-extra '{"bootstrap.servers": "kafka:29092", "group.id": "airflow-consumer", "security.protocol": "PLAINTEXT"}' \
    || echo "   → Conexión 'kafka_default' ya existe."

# ── PostgreSQL (analytics) ───────────────────────────────────────────────────
airflow connections add postgres_default \
    --conn-type postgres \
    --conn-host postgres \
    --conn-port 5432 \
    --conn-login airflow \
    --conn-password airflow \
    --conn-schema airflow \
    || echo "   → Conexión 'postgres_default' ya existe."

# ── WebHDFS (NameNode REST API) ──────────────────────────────────────────────
# Usada por el HttpSensor para detectar la llegada de ficheros desde Kafka Connect
airflow connections add webhdfs_default \
    --conn-type http \
    --conn-host namenode \
    --conn-port 9870 \
    --conn-schema http \
    || echo "   → Conexión 'webhdfs_default' ya existe."

# ── Kafka Connect REST API ───────────────────────────────────────────────────
airflow connections add kafka_connect_default \
    --conn-type http \
    --conn-host kafka-connect \
    --conn-port 8083 \
    --conn-schema http \
    || echo "   → Conexión 'kafka_connect_default' ya existe."

echo "[5/5] Configurando Variables de Airflow..."
# BATCH_SIZE   → número de filas a procesar por ejecución del DAG
# DELAY_SECONDS → pausa (seg) entre el envío de cada mensaje a Kafka
airflow variables set BATCH_SIZE 50     || true
airflow variables set DELAY_SECONDS 0.1 || true

echo ""
echo "======================================================="
echo " Configuración completada. Arrancando servicios..."
echo "   WebUI:  http://localhost:8081  (admin / admin)"
echo "======================================================="

echo "Arrancando Airflow Scheduler en background..."
airflow scheduler &

echo "Arrancando Airflow Webserver en foreground (puerto 8080)..."
exec airflow webserver --port 8080
