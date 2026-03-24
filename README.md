# Entregable 3 — Home Sensor Pipeline
**Máster en Ingeniería de Datos**

Pipeline de datos completo orquestado con Apache Airflow que ingiere lecturas
de sensores domésticos, las publica en Kafka, las persiste en HDFS mediante
Kafka Connect, las transforma con Pandas y las analiza en PostgreSQL.

---

## Arquitectura

```
CSV ──► Kafka ──► Kafka Connect ──► HDFS (Namenode + 2 Datanodes)
                                          │
                                          ▼
                              Airflow (Pandas transform)
                                          │
                                          ▼
                                    PostgreSQL
                                          │
                                          ▼
                              logs_analytics/reporte_*.log
```

### Servicios Docker

| Servicio       | Imagen                                    | Puerto(s) host |
|----------------|-------------------------------------------|----------------|
| `postgres`     | postgres:15                               | 5432           |
| `kafka`        | confluentinc/cp-kafka:7.9.0               | 9092           |
| `namenode`     | bde2020/hadoop-namenode:2.0.0-hadoop3.2.1 | 9870, 9000     |
| `datanode1`    | bde2020/hadoop-datanode:2.0.0-hadoop3.2.1 | 9864           |
| `datanode2`    | bde2020/hadoop-datanode:2.0.0-hadoop3.2.1 | 9865           |
| `kafka-connect`| confluentinc/cp-kafka-connect:7.9.0       | 8083           |
| `airflow`      | custom (Airflow 2.9.3 + providers)        | 8081           |

---

## Estructura del proyecto

```
entregable/
├── docker-compose.yml
├── Dockerfile.airflow
├── init-airflow.sh
├── README.md
├── data/
│   └── home_temperature_and_humidity_smoothed_filled.csv
├── dags/
│   ├── sensor_pipeline_dag.py     ← Grafo del DAG (solo dependencias)
│   └── tasks/
│       ├── config.py              ← Constantes globales (hosts, rutas, etc.)
│       ├── t01_ingest.py          ← Lectura CSV por lotes
│       ├── t02_kafka.py           ← Publicación en Kafka
│       ├── t03_connector.py       ← Registro idempotente del conector HDFS
│       ├── t04_sensor.py          ← Función de verificación para HttpSensor
│       ├── t05_transform.py       ← Transformación Pandas (sin Spark)
│       ├── t06_postgres.py        ← DDL + carga masiva PostgreSQL
│       └── t07_report.py          ← Analíticas + generación de reporte
├── logs_analytics/                ← Reportes JSON generados por el DAG
└── scripts/
    └── register_connector.sh      ← Registro manual del conector (cURL)
```

---

## Guía de ejecución paso a paso

### Requisitos previos

- Docker Desktop ≥ 4.x (con al menos **6 GB de RAM** asignados)
- Docker Compose V2
- Acceso a internet el primer arranque (descarga del plugin HDFS3)

---

### Paso 1 — Arrancar la infraestructura

```bash
cd entregable/

# Construir la imagen de Airflow e iniciar todos los servicios
docker compose up --build -d
```

> **Primera ejecución:** el servicio `kafka-connect` descarga el plugin
> `confluentinc/kafka-connect-hdfs3` desde Confluent Hub (~200 MB).
> Esto tarda **2-4 minutos**. Los siguientes arranques son inmediatos.

---

### Paso 2 — Verificar que los servicios están sanos

```bash
docker compose ps
```

Espera hasta que **todos** los servicios aparezcan como `healthy` o `running`.
En particular, `kafka-connect` tarda más por la instalación del plugin.

```bash
# Ver logs de un servicio en concreto
docker compose logs -f kafka-connect
docker compose logs -f airflow
```

---

### Paso 3 — Abrir la UI de Airflow

Navega a **http://localhost:8081**

| Campo    | Valor |
|----------|-------|
| Usuario  | admin |
| Password | admin |

---

### Paso 4 — (Opcional) Ajustar el tamaño del lote y la velocidad

En Airflow UI → **Admin → Variables**, puedes modificar:

| Variable       | Descripción                              | Default |
|----------------|------------------------------------------|---------|
| `BATCH_SIZE`   | Filas del CSV procesadas por ejecución   | `50`    |
| `DELAY_SECONDS`| Pausa (segundos) entre mensajes a Kafka  | `0.1`   |

Para un test rápido usa `BATCH_SIZE=20` y `DELAY_SECONDS=0`.

---

### Paso 5 — Ejecutar el DAG

1. En la UI, localiza el DAG **`sensor_pipeline_dag`**.
2. Actívalo con el toggle si está en pausa.
3. Pulsa **▶ Trigger DAG** (el icono de play).
4. Sigue el progreso en la vista **Graph** o **Grid**.

El flujo de tareas es:

```
01_ingest_csv_batch
      │
02_publish_to_kafka
      │
03_register_hdfs_connector
      │
04_wait_for_hdfs_data  ← espera activa hasta que Kafka Connect descargue
      │                   (~30-60 s tras publicar en Kafka)
05_transform_normalize
      │
06a_create_postgres_table
      │
06b_load_to_postgres
      │
07_08_analytics_and_report
```

---

### Paso 6 — Revisar los resultados

**Reporte analítico** (generado automáticamente por la última tarea):

```bash
# En tu máquina host, dentro de entregable/
cat logs_analytics/reporte_*.log
```

**Datos en PostgreSQL:**

```bash
docker exec -it postgres psql -U airflow -d airflow -c \
  "SELECT timestamp, avg_indoor_temperature, avg_indoor_humidity, temperature_exterieur
   FROM home_sensor_readings ORDER BY timestamp LIMIT 10;"
```

**Ficheros en HDFS** (Web UI del Namenode):

Abre **http://localhost:9870/explorer.html#/user/kafka/home_sensors**

También puedes usar la API WebHDFS directamente:

```bash
curl -s "http://localhost:9870/webhdfs/v1/user/kafka/home_sensors?op=LISTSTATUS" | python -m json.tool
```

**Topics en Kafka:**

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic home_sensors \
  --from-beginning \
  --max-messages 5
```

---

### Paso 7 — (Opcional) Registrar el conector manualmente

Si quieres registrar o actualizar el conector sin pasar por Airflow:

```bash
chmod +x scripts/register_connector.sh
./scripts/register_connector.sh              # usa localhost:8083 por defecto
```

También puedes hacerlo con cURL directamente:

```bash
# Verificar plugins disponibles (debe aparecer Hdfs3SinkConnector)
curl -s http://localhost:8083/connector-plugins | python -m json.tool

# Ver el estado del conector tras ejecutar el DAG
curl -s http://localhost:8083/connectors/hdfs3-sink-home-sensors/status | python -m json.tool

# Reiniciar el conector si está en estado FAILED
curl -X POST http://localhost:8083/connectors/hdfs3-sink-home-sensors/restart
```

---

### Paso 8 — Apagar la infraestructura

```bash
# Parar los contenedores (conserva los datos en los volúmenes)
docker compose down

# Parar Y eliminar todos los datos (volúmenes incluidos)
docker compose down -v
```

---

## Analíticas implementadas

El DAG genera automáticamente un reporte en `logs_analytics/` con tres secciones:

### A — Detección de ineficiencia térmica
Detecta momentos donde la **temperatura exterior < 15 °C** pero la
**temperatura interior media > 23 °C**. Señal de que la calefacción está
encendida mientras hay ventanas abiertas (derroche energético).

### B — Calidad del aire crítica
Detecta zonas del hogar donde el **índice de aire supera 1500 ppm** (CO₂ elevado).
Clasifica la zona afectada: salón, habitación, despacho, o combinaciones.
Indica la necesidad de ventilación inmediata.

### C — Resumen estadístico
Temperaturas medias/mín/máx interiores y exteriores, humedad media y
calidad del aire media del periodo completo cargado en BD.

---

## Personalización

| Qué cambiar | Dónde |
|---|---|
| Hosts / puertos de servicios | `dags/tasks/config.py` |
| Parámetros del conector HDFS | `dags/tasks/t03_connector.py` → `_CONNECTOR_CONFIG` |
| Umbrales analíticos | `dags/tasks/t07_report.py` (constantes en las queries SQL) |
| Columnas de la tabla | `dags/tasks/t06_postgres.py` → `CREATE_TABLE_SQL` y `_COL_ORDER` |
| Tamaño del lote / velocidad | Variables de Airflow: `BATCH_SIZE`, `DELAY_SECONDS` |

---

## Solución de problemas frecuentes

| Síntoma | Causa probable | Solución |
|---|---|---|
| `kafka-connect` no aparece `healthy` | Plugin HDFS3 aún descargándose | Espera 3-5 min y reintenta |
| Tarea `04_wait_for_hdfs_data` espera indefinidamente | Conector FAILED o Kafka sin mensajes | `curl localhost:8083/connectors/hdfs3-sink-home-sensors/status` |
| `KafkaError: NoBrokersAvailable` | Kafka aún arrancando | Reinicia la tarea desde la UI |
| Error en `06b_load_to_postgres` | Tabla no creada aún | Asegúrate de que `06a` terminó en verde |
| Reporte no aparece en `logs_analytics/` | Permisos de escritura | `docker exec airflow chmod 777 /opt/airflow/logs_analytics` |
