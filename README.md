# Proyecto Airflow + PostgreSQL + Kafka

Este repositorio contiene un entorno de demostracion basado en Docker para trabajar con Apache Airflow, PostgreSQL y Apache Kafka. El proyecto esta pensado para ejecutar DAGs de ejemplo que muestran tareas basicas, comunicacion entre tareas con XCom, publicacion de mensajes en Kafka y operaciones simples sobre PostgreSQL.

## Objetivo del proyecto

El entorno levanta una pila minima con:

- Apache Airflow como orquestador.
- PostgreSQL como base de datos de metadatos de Airflow.
- Kafka en modo KRaft.

Ademas, el contenedor de Airflow crea automaticamente:

- El usuario administrador `admin`.
- La conexion `kafka_default`.
- La conexion `postgres_default`.

## Arquitectura

Los servicios definidos en `docker-compose.yml` son:

- `postgres`: base de datos PostgreSQL 15 para Airflow.
- `kafka`: broker Kafka con imagen `confluentinc/cp-kafka:7.9.0`.
- `airflow`: contenedor personalizado a partir de `apache/airflow:2.9.3-python3.9`.

Red y volumenes:

- Red Docker: `airflow_network`.
- Volumen persistente para PostgreSQL: `postgres-db`.
- Volumen persistente para logs de Airflow: `airflow-logs`.

## Puertos y accesos

- Airflow Web UI: `http://localhost:8081`
- PostgreSQL: `localhost:5432`
- Kafka desde el host: `localhost:9092`
- Kafka dentro de la red Docker: `kafka:29092`

Credenciales de Airflow:

- Usuario: `admin`
- Clave: `admin`

## Estructura del proyecto

```text
.
|-- docker-compose.yml
|-- Dockerfile.airflow
|-- README.md
|-- airflow/
|   |-- airflow.cfg
|   |-- webserver_config.py
|   |-- dags/
|   |   |-- demo_01_basic.py
|   |   |-- demo_02_xcom.py
|   |   |-- demo_03_kafka.py
|   |   `-- demo_04_postgres.py
|   `-- airflow-webserver.pid
`-- src/
	`-- dags_demo/
		|-- dag_hello.py
		|-- dag_kafka.py
		`-- dag_random.py
```

## Carpetas y archivos importantes

### `docker-compose.yml`

Define toda la infraestructura del entorno y el arranque del contenedor de Airflow. En este archivo tambien se monta la carpeta activa de DAGs:

- `./airflow/dags:/opt/airflow/dags`

Eso significa que los DAGs que Airflow ejecuta actualmente son los de `airflow/dags`.

### `Dockerfile.airflow`

Construye la imagen personalizada de Airflow e instala:

- `apache-airflow==2.9.3`
- `apache-airflow-providers-postgres`
- `apache-airflow-providers-apache-kafka`
- `psycopg2-binary`

### `airflow/dags/`

Contiene los DAGs activos del proyecto.

### `src/dags_demo/`

Contiene DAGs adicionales de ejemplo, pero actualmente no se montan en el contenedor segun la configuracion de `docker-compose.yml`. Sirven como material de referencia o versiones alternativas.

## DAGs activos

Los DAGs que Airflow carga actualmente son los ubicados en `airflow/dags`.

### `demo_01_basic`

DAG basico con tres tareas:

- `start` con `EmptyOperator`
- `hello` con `BashOperator`
- `end` con `EmptyOperator`

La tarea central imprime el mensaje `Hola desde Airflow`.

### `demo_02_xcom`

Demuestra comunicacion entre tareas usando XCom:

- `generar_numero` devuelve el valor `7`.
- `consumir_numero` recupera ese valor con `xcom_pull` y muestra el resultado multiplicado por `10`.

### `demo_03_kafka`

Demuestra produccion de mensajes a Kafka usando `ProduceToTopicOperator`.

Caracteristicas:

- Topic usado: `demo_airflow_topic`
- Conexion Airflow: `kafka_default`
- Publica tres mensajes JSON de ejemplo.

### `demo_04_postgres`

Demuestra interaccion con PostgreSQL usando `PostgresHook`.

Flujo:

- Crea la tabla `demo_airflow_users` si no existe.
- Inserta tres registros de ejemplo.
- Consulta e imprime el contenido ordenado por `id`.

Conexion Airflow utilizada:

- `postgres_default`

## DAGs de referencia en `src/dags_demo`

Estos archivos no se ejecutan automaticamente con la configuracion actual, pero forman parte del proyecto:

### `dag_hello.py`

Ejemplo sencillo con `BashOperator` y ejecucion diaria.

### `dag_kafka.py`

Ejemplo alternativo de integracion con Kafka:

- Usa el topic `test_topic`.
- Produce 10 mensajes JSON.
- Cuenta cuantos mensajes fueron generados a traves de XCom.

### `dag_random.py`

Ejemplo con:

- Una tarea Bash para imprimir un mensaje.
- Una tarea Python para generar un numero aleatorio.

## Como iniciar el proyecto

Ejecuta:

```bash
docker compose up -d --build
```

Esto construye la imagen de Airflow, levanta PostgreSQL y Kafka, inicializa la base de datos de Airflow y arranca el scheduler y el webserver.

## Como detener el proyecto

```bash
docker compose down
```

Para borrar tambien los volumenes y reiniciar el entorno desde cero:

```bash
docker compose down -v
```

## Uso basico

1. Levanta los servicios con Docker Compose.
2. Abre Airflow en `http://localhost:8081`.
3. Inicia sesion con `admin / admin`.
4. Activa el DAG que quieras probar.
5. Ejecuta el DAG manualmente desde la interfaz.

## Comandos utiles para Kafka

Crear el topic usado por el DAG activo de Kafka:

```bash
docker compose exec kafka kafka-topics --create --topic demo_airflow_topic --partitions 1 --replication-factor 1 --bootstrap-server kafka:29092
```

Si quieres probar el DAG alternativo de `src/dags_demo/dag_kafka.py`, el topic esperado es `test_topic`:

```bash
docker compose exec kafka kafka-topics --create --topic test_topic --partitions 1 --replication-factor 1 --bootstrap-server kafka:29092
```

Listar topics:

```bash
docker compose exec kafka kafka-topics --list --bootstrap-server kafka:29092
```

## Comandos utiles para PostgreSQL

Abrir consola `psql` dentro del contenedor:

```bash
docker compose exec postgres psql -U airflow -d airflow
```

Consultar la tabla creada por `demo_04_postgres`:

```sql
SELECT * FROM demo_airflow_users ORDER BY id;
```

## Detalles tecnicos relevantes

- Airflow usa `LocalExecutor`.
- La base de datos de Airflow se conecta con `postgresql+psycopg2://airflow:airflow@postgres:5432/airflow`.
- El proyecto desactiva los ejemplos por defecto de Airflow con `AIRFLOW__CORE__LOAD_EXAMPLES=False`.
- Kafka funciona en modo KRaft, por lo que no requiere ZooKeeper.

## Observaciones

- El README anterior indicaba que los DAGs se montaban desde `src/dags_demo`, pero la configuracion real monta `airflow/dags`.
- La carpeta `src/dags_demo` sigue siendo util como coleccion de ejemplos, aunque no forma parte del despliegue activo.
- Dentro de `airflow/` tambien aparecen archivos generados por la ejecucion local de Airflow, como `airflow-webserver.pid` y `airflow.db`.

## Resumen

Este proyecto sirve como laboratorio base para practicar orquestacion con Airflow y su integracion con servicios externos. Incluye ejemplos listos para ejecutar con PostgreSQL y Kafka, ademas de DAGs adicionales de referencia para extender o adaptar el entorno.
