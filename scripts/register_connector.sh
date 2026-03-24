#!/bin/bash
# Registra el HDFS3 Sink Connector en Kafka Connect (idempotente).
# Uso: ./register_connector.sh [CONNECT_HOST:PORT] [NAMENODE_HOST:PORT]
set -e

CONNECT_URL="http://${1:-localhost:8083}"
HDFS_URL="hdfs://${2:-namenode:9000}"
NAME="hdfs3-sink-home-sensors"

CONFIG=$(cat <<EOF
{
  "connector.class": "io.confluent.connect.hdfs3.Hdfs3SinkConnector",
  "tasks.max": "1",
  "topics": "home_sensors",
  "hdfs.url": "${HDFS_URL}",
  "storage.class": "io.confluent.connect.hdfs3.storage.HdfsStorage",
  "format.class":  "io.confluent.connect.hdfs3.json.JsonFormat",
  "topics.dir": "/user/kafka",
  "flush.size": "50",
  "rotate.interval.ms": "30000",
  "rotate.schedule.interval.ms": "60000",
  "schema.compatibility": "NONE",
  "locale": "en_US",
  "timezone": "UTC",
  "hdfs.authentication.kerberos": "false"
}
EOF
)

# Comprobar disponibilidad
curl -sf "${CONNECT_URL}/connectors" > /dev/null \
  || { echo "ERROR: Kafka Connect no disponible en ${CONNECT_URL}"; exit 1; }

# Crear o actualizar según exista
if curl -sf "${CONNECT_URL}/connectors/${NAME}" > /dev/null; then
  echo "Actualizando conector existente..."
  curl -s -X PUT  -H "Content-Type: application/json" \
    -d "${CONFIG}" "${CONNECT_URL}/connectors/${NAME}/config"
else
  echo "Creando nuevo conector..."
  curl -s -X POST -H "Content-Type: application/json" \
    -d "{\"name\":\"${NAME}\",\"config\":${CONFIG}}" "${CONNECT_URL}/connectors"
fi

# Estado final
sleep 3
STATE=$(curl -s "${CONNECT_URL}/connectors/${NAME}/status" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null \
  || echo "UNKNOWN")

echo ""
echo "Estado: ${STATE}"
[ "${STATE}" = "RUNNING" ] \
  && echo "OK — datos en HDFS: /user/kafka/home_sensors/partition=0/" \
  || echo "WARN — revisa: docker logs kafka-connect"
