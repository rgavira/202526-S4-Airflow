<a name="readme-top"></a>
# Ingeniería de Datos: Big Data - S4-Airflow

Este repositorio contiene el entorno de desarrollo para **Apache Airflow** utilizado en la **Sesión 4** de la asignatura **Ingeniería de Datos: Big Data** del **Máster en Ingeniería del Software - Cloud, Datos y Gestión TI** de la Escuela Técnica Superior de Ingeniería Informática de la Universidad de Sevilla.

## 🚀 Características  

- 📌 **Entorno basado en Docker**: Fácil despliegue y configuración de todos los servicios.
- 📁 **Hadoop HDFS**: Almacenamiento distribuido de archivos con NameNode y 2 DataNodes.
- 🔧 **YARN**: Gestor de recursos para procesamiento distribuido.
- 🔍 **Hive**: Motor de consultas SQL sobre datos distribuidos en HDFS con metastore en MySQL.
- 📡 **Apache Kafka**: Procesamiento de eventos en tiempo real con Zookeeper y Kafka UI.
- 🚀 **Apache Airflow**: Orquestación de flujos de datos (DAGs) con LocalExecutor y PostgreSQL.
- 🔄 **Integración completa**: Airflow preconfigurado para interactuar con HDFS, Hive y Kafka.
- 💊 **Health Checks**: Monitorización automática del estado de todos los servicios.
- 📦 **Persistencia de datos**: Volúmenes Docker para mantener datos entre reinicios.

## 📂 Estructura del Proyecto  

```
📂 S4-Airflow/
├── 📄 docker-compose.yml           # Orquestación de todos los servicios
├── 📄 dockerfile                   # Imagen base de Hadoop/Hive
├── 📄 Dockerfile.airflow          # Imagen personalizada de Airflow
├── 📄 .dockerignore               # Archivos excluidos del build
├── 📄 .gitignore                  # Archivos excluidos del repositorio
├── 📄 README.md                    # Documentación del proyecto
│
├── 📂 airflow/                     # Configuración de Airflow
│   ├── airflow-webserver.pid
│   ├── airflow.cfg                # Configuración principal de Airflow
│   └── webserver_config.py
│
├── 📂 hadoop_config/               # Configuración de Hadoop/YARN
│   ├── capacity-scheduler.xml
│   ├── core-site.xml              # Configuración del core de Hadoop
│   ├── hdfs-site.xml              # Configuración de HDFS
│   ├── log4j.properties
│   ├── mapred-site.xml            # Configuración de MapReduce
│   └── yarn-site.xml              # Configuración de YARN
│
├── 📂 hive/                        # Configuración de Hive
│   └── 📂 conf/
│       ├── core-site.xml
│       ├── hive-log4j2.properties
│       └── hive-site.xml          # Configuración del metastore
│
├── 📂 mysql/                       # Scripts de inicialización de MySQL
│   └── init.sql                   # Creación de BD y usuario para Hive
│
├── 📂 scripts/                     # Scripts de inicialización
│   ├── init-datanode.sh           # Inicialización de DataNodes
│   ├── start-hdfs.sh              # Inicio del NameNode
│   ├── start-nodemanager.sh       # Inicio del NodeManager
│   └── start-yarn.sh              # Inicio del ResourceManager
│
└── 📂 src/                         # Código fuente y DAGs
    ├── example_hdfs.py
    └── 📂 dags_demo/               # DAGs de ejemplo para Airflow
        ├── dag_hdfs.py            # Operaciones con HDFS
        ├── dag_hello.py           # DAG de ejemplo básico
        ├── dag_hive.py            # Consultas con Hive
        ├── dag_kafka.py           # Integración con Kafka
        └── dag_random.py          # Generación de datos aleatorios
```

## 🛠️ Requisitos  

- **Docker** (versión 20.10 o superior) y **Docker Compose** (versión 2.0 o superior)
- **Sistema Operativo**: Windows 10/11, Linux, o macOS
- **RAM**: Mínimo 8GB (recomendado 16GB+ para ejecutar todos los servicios)
- **Espacio en disco**: Al menos 20GB libres para imágenes, contenedores y datos
- **Puertos libres**: 2181, 8020, 8030-8033, 8040, 8042, 8080-8081, 8088, 9000, 9083, 9092-9093, 9864-9867, 9870, 10000, 10002

## ⚡ Instalación y Uso  

### 1️⃣ Clonar el repositorio

```sh
git clone https://github.com/josemarialuna/hdfs-docker-cluster.git
cd S4-Airflow
```

### 2️⃣ Construir las imágenes Docker

```sh
docker-compose build
```

Este proceso puede tardar varios minutos la primera vez, ya que descarga Hadoop, Hive y todas las dependencias necesarias.

### 3️⃣ Iniciar todos los servicios

```sh
docker-compose up -d
```

Los servicios se iniciarán en el siguiente orden:
1. **Zookeeper** → **Kafka** → **Kafka UI**
2. **PostgreSQL** (para Airflow)
3. **MySQL** (para Hive Metastore)
4. **NameNode** → **DataNodes** (HDFS)
5. **ResourceManager** → **NodeManager** (YARN)
6. **Hive Metastore** → **HiveServer2**
7. **Airflow**

### 4️⃣ Verificar que los contenedores están en ejecución

```sh
docker-compose ps
```

Deberías ver todos los servicios con estado `Up` y los health checks en `healthy`.

### 5️⃣ Acceder a las interfaces web

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| **Airflow** | http://localhost:8081 | Usuario: `admin` / Contraseña: `admin` |
| **HDFS NameNode** | http://localhost:9870 | - |
| **YARN ResourceManager** | http://localhost:8088 | - |
| **NodeManager** | http://localhost:8042 | - |
| **Kafka UI** | http://localhost:8080 | - |
| **HiveServer2 Web UI** | http://localhost:10002 | - |

### 6️⃣ Detener todos los servicios

```sh
docker-compose down
```

Para eliminar también los volúmenes de datos:

```sh
docker-compose down -v
```

## 📌 Comandos Útiles  

### HDFS

**Acceder al contenedor del NameNode:**
```sh
docker exec -it namenode bash
```

**Listar los archivos en HDFS:**
```sh
hdfs dfs -ls /
```

**Crear un directorio en HDFS:**
```sh
hdfs dfs -mkdir -p /user/airflow/data
```

**Subir un archivo a HDFS:**
```sh
hdfs dfs -put archivo.txt /user/airflow/data/
```

**Descargar un archivo de HDFS:**
```sh
hdfs dfs -get /user/airflow/data/archivo.txt .
```

**Ver el contenido de un archivo:**
```sh
hdfs dfs -cat /user/airflow/data/archivo.txt
```

**Ver el estado del clúster:**
```sh
hdfs dfsadmin -report
```

**Salir manualmente del Safe Mode:**
```sh
hdfs dfsadmin -safemode leave
```

**Ver el espacio utilizado:**
```sh
hdfs dfs -du -h /
```

### YARN

**Acceder al contenedor del ResourceManager:**
```sh
docker exec -it resourcemanager bash
```

**Listar aplicaciones en ejecución:**
```sh
yarn application -list
```

**Ver el estado de una aplicación:**
```sh
yarn application -status <application_id>
```

**Matar una aplicación:**
```sh
yarn application -kill <application_id>
```

### Hive

**Acceder al contenedor de HiveServer2:**
```sh
docker exec -it hiveserver2 bash
```

**Conectar a Hive desde la línea de comandos:**
```sh
beeline -u jdbc:hive2://localhost:10000
```

**Ejecutar una consulta SQL:**
```sql
CREATE TABLE IF NOT EXISTS usuarios (
    id INT,
    nombre STRING,
    edad INT
) STORED AS PARQUET;

INSERT INTO usuarios VALUES (1, 'Juan', 30), (2, 'María', 25);

SELECT * FROM usuarios;
```

**Verificar el metastore:**
```sh
docker exec -it hive-metastore-db mysql -u hive -phivepassword -e "USE metastore; SHOW TABLES;"
```

### Kafka

**Acceder al contenedor de Kafka:**
```sh
docker exec -it kafka bash
```

**Crear un topic:**
```sh
kafka-topics --create --topic mi_topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

**Listar topics:**
```sh
kafka-topics --list --bootstrap-server localhost:9092
```

**Describir un topic:**
```sh
kafka-topics --describe --topic mi_topic --bootstrap-server localhost:9092
```

**Producir mensajes:**
```sh
kafka-console-producer --topic mi_topic --bootstrap-server localhost:9092
```

**Consumir mensajes:**
```sh
kafka-console-consumer --topic mi_topic --from-beginning --bootstrap-server localhost:9092
```

**Eliminar un topic:**
```sh
kafka-topics --delete --topic mi_topic --bootstrap-server localhost:9092
```

### Airflow

**Acceder al contenedor de Airflow:**
```sh
docker exec -it airflow bash
```

**Listar DAGs disponibles:**
```sh
airflow dags list
```

**Ver el estado de un DAG:**
```sh
airflow dags state <dag_id> <execution_date>
```

**Ejecutar un DAG manualmente:**
```sh
airflow dags trigger <dag_id>
```

**Ejecutar una tarea específica:**
```sh
airflow tasks test <dag_id> <task_id> <execution_date>
```

**Ver los logs de una tarea:**
```sh
airflow tasks logs <dag_id> <task_id> <execution_date>
```

**Pausar/Despausar un DAG:**
```sh
airflow dags pause <dag_id>
airflow dags unpause <dag_id>
```

**Crear un nuevo usuario:**
```sh
airflow users create \
    --username nuevo_usuario \
    --password password123 \
    --firstname Nombre \
    --lastname Apellido \
    --role Admin \
    --email usuario@example.com
```

## 🔧 Arquitectura del Sistema

El sistema está compuesto por los siguientes componentes interconectados:

```
┌─────────────────────────────────────────────────────────────────┐
│                         AIRFLOW (Puerto 8081)                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  Webserver   │  │  Scheduler   │  │  PostgreSQL  │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└────────────┬────────────────┬────────────────┬──────────────────┘
             │                │                │
             ▼                ▼                ▼
    ┌────────────────┐ ┌─────────────┐ ┌──────────────┐
    │ HDFS (9870)    │ │ Hive (10000)│ │ Kafka (9092) │
    │  ┌──────────┐  │ │ ┌─────────┐ │ │ ┌──────────┐ │
    │  │ NameNode │  │ │ │HiveServer│ │ │ │  Broker  │ │
    │  └──────────┘  │ │ └─────────┘ │ │ └──────────┘ │
    │  ┌──────────┐  │ │ ┌─────────┐ │ │ ┌──────────┐ │
    │  │DataNode1 │  │ │ │Metastore│ │ │ │Zookeeper │ │
    │  └──────────┘  │ │ └─────────┘ │ │ └──────────┘ │
    │  ┌──────────┐  │ │ ┌─────────┐ │ │              │
    │  │DataNode2 │  │ │ │  MySQL  │ │ │              │
    │  └──────────┘  │ │ └─────────┘ │ │              │
    └────────────────┘ └─────────────┘ └──────────────┘
             │                │                │
             ▼                ▼                ▼
    ┌────────────────────────────────────────────────┐
    │      YARN ResourceManager (Puerto 8088)        │
    │  ┌──────────────────────────────────────────┐  │
    │  │        NodeManager (Puerto 8042)         │  │
    │  └──────────────────────────────────────────┘  │
    └────────────────────────────────────────────────┘
```

### Flujo de datos típico:

1. **Airflow** orquesta los flujos de trabajo (DAGs)
2. **Kafka** recibe y procesa eventos en tiempo real
3. Los datos se almacenan en **HDFS** a través del NameNode
4. **Hive** permite consultar los datos con SQL
5. **YARN** gestiona los recursos para procesamiento distribuido

## 📊 DAGs de Ejemplo

El proyecto incluye varios DAGs de ejemplo en `src/dags_demo/`:

### dag_hello.py
DAG básico que imprime "Hello World" para verificar que Airflow funciona correctamente.

### dag_hdfs.py
Operaciones con HDFS:
- Crear directorios
- Subir archivos
- Listar contenidos
- Descargar archivos

### dag_hive.py
Operaciones con Hive:
- Crear tablas
- Insertar datos
- Ejecutar consultas SQL
- Exportar resultados

### dag_kafka.py
Operaciones con Kafka:
- Crear topics
- Producir mensajes
- Consumir mensajes
- Procesar eventos

### dag_random.py
Generación de datos aleatorios y almacenamiento en HDFS/Hive.

## 🔒 Seguridad

**⚠️ IMPORTANTE**: Este entorno está diseñado para **desarrollo y pruebas**, NO para producción.

- Las contraseñas están en texto plano en el `docker-compose.yml`
- HDFS tiene los permisos deshabilitados (`dfs.permissions.enabled=false`)
- No hay encriptación de datos en tránsito ni en reposo
- Los puertos están expuestos sin autenticación adicional

Para un entorno de producción:
- Utiliza secretos de Docker o herramientas de gestión de secretos
- Habilita Kerberos para Hadoop
- Configura SSL/TLS para todas las comunicaciones
- Implementa firewalls y segmentación de red
- Activa la auditoría y monitorización

## 📝 Notas Técnicas

### Recursos del Sistema

Cada servicio tiene asignados recursos específicos:
- **NameNode**: ~1GB RAM
- **DataNode**: ~512MB RAM cada uno
- **ResourceManager**: ~1GB RAM
- **NodeManager**: 2GB RAM, 2 vCores
- **HiveServer2**: ~1GB RAM
- **Kafka**: ~1GB RAM
- **Airflow**: ~1GB RAM
- **PostgreSQL/MySQL**: ~256MB RAM cada uno

**Total aproximado**: 8-10GB RAM

### Persistencia de Datos

Los siguientes volúmenes Docker mantienen los datos:
- `namenode-data`: Metadatos de HDFS
- `datanode1-data`, `datanode2-data`: Bloques de datos de HDFS
- `hive-metastore-db-data`: Metadatos de Hive
- `kafka-data`: Mensajes de Kafka
- `zookeeper-data`, `zookeeper-logs`: Estado de Zookeeper
- `postgres-data`: Base de datos de Airflow
- `airflow-logs`: Logs de ejecución de DAGs

### Configuración de Airflow

- **Executor**: LocalExecutor (para desarrollo)
- **Base de datos**: PostgreSQL 14
- **DAGs**: Se cargan desde `src/dags_demo/`
- **Conexiones preconfiguradas**:
  - `HDFS_DEFAULT`: hdfs://namenode:9000
  - `HIVE_CLI_DEFAULT`: hive_cli://hiveserver2:10000
  - `KAFKA_DEFAULT`: kafka://kafka:9092

### Health Checks

Todos los servicios críticos tienen health checks configurados:
- NameNode: Verifica interfaz web (puerto 9870)
- ResourceManager: Verifica API del cluster (puerto 8088)
- MySQL: Comando `mysqladmin ping`
- PostgreSQL: Comando `pg_isready`
- Kafka: Verifica API de brokers
- Hive Metastore: Verifica puerto Thrift (9083)
- Airflow: Verifica endpoint `/health`

## ❓ Preguntas Frecuentes (FAQ)

### ¿Por qué el NameNode tarda tanto en iniciar?

El NameNode debe formatear el sistema de archivos la primera vez y esperar a salir del "Safe Mode". Esto puede tardar 1-2 minutos. Verifica los logs:

```sh
docker logs namenode
```

### ¿Cómo puedo ver los logs de un servicio?

```sh
docker logs <nombre_contenedor>
```

Para seguir los logs en tiempo real:

```sh
docker logs -f <nombre_contenedor>
```

### El NameNode muestra "unexpected end of file"

Esto indica caracteres de fin de línea incorrectos (Windows CRLF vs Unix LF). Convierte los scripts:

```sh
docker exec -it namenode bash
sed -i 's/\r$//' /scripts/start-hdfs.sh
```

O antes de ejecutar Docker Compose:

```sh
dos2unix scripts/*.sh
```

### ¿Cómo reinicio un servicio específico?

```sh
docker-compose restart <nombre_servicio>
```

Por ejemplo:
```sh
docker-compose restart airflow
```

### Los contenedores no se comunican entre sí

Verifica que todos están en la misma red:

```sh
docker network inspect hadoop-network
```

Verifica la resolución DNS:

```sh
docker exec -it airflow ping namenode
```

### ¿Cómo actualizo las imágenes?

```sh
docker-compose pull
docker-compose build --no-cache
docker-compose up -d
```

### El puerto 8080 está ocupado

Kafka UI usa el puerto 8080 por defecto. Si ya lo tienes ocupado, modifica en `docker-compose.yml`:

```yaml
kafka-ui:
  ports:
    - "8090:8080"  # Cambia 8090 por el puerto que prefieras
```

### ¿Cómo limpio completamente el entorno?

```sh
# Detener y eliminar contenedores
docker-compose down

# Eliminar volúmenes
docker-compose down -v

# Eliminar imágenes
docker rmi $(docker images 's4-airflow*' -q)

# Limpiar sistema completo (¡cuidado!)
docker system prune -a --volumes
```

### ¿Puedo usar este proyecto en producción?

**NO**. Este entorno está configurado para desarrollo y aprendizaje. Para producción necesitarás:
- Configuración de seguridad (Kerberos, SSL/TLS)
- Alta disponibilidad (múltiples NameNodes, ResourceManagers)
- Monitorización y alertas (Prometheus, Grafana)
- Backups automatizados
- Gestión de secretos segura
- Dimensionamiento adecuado de recursos

## 🐛 Solución de Problemas

### Los DataNodes no se registran

**Síntoma**: El NameNode muestra 0 DataNodes en http://localhost:9870

**Solución**:
```sh
# Verifica que el NameNode salió del Safe Mode
docker exec -it namenode hdfs dfsadmin -safemode get

# Fuerza la salida del Safe Mode
docker exec -it namenode hdfs dfsadmin -safemode leave

# Reinicia los DataNodes
docker-compose restart datanode1 datanode2
```

### Hive no puede conectarse al metastore

**Síntoma**: Errores de conexión a MySQL

**Solución**:
```sh
# Verifica que MySQL está corriendo
docker logs hive-metastore-db

# Verifica la conexión desde Hive
docker exec -it hive-metastore mysql -h hive-metastore-db -u hive -phivepassword -e "SHOW DATABASES;"

# Reinicializa el schema si es necesario
docker exec -it hive-metastore /opt/hive/bin/schematool -dbType mysql -initSchema
```

### Kafka no puede crear topics

**Síntoma**: Errores al crear topics desde Airflow o la línea de comandos

**Solución**:
```sh
# Verifica que Zookeeper está corriendo
docker logs zookeeper

# Verifica que Kafka está conectado a Zookeeper
docker logs kafka | grep -i "zookeeper"

# Reinicia Kafka
docker-compose restart kafka
```

### Airflow no muestra los DAGs

**Síntoma**: La interfaz web está vacía

**Solución**:
```sh
# Verifica que los DAGs están montados
docker exec -it airflow ls -la /opt/airflow/dags

# Verifica errores de sintaxis en los DAGs
docker exec -it airflow airflow dags list-import-errors

# Recarga los DAGs
docker exec -it airflow airflow dags reserialize
```

### Problemas de espacio en disco

**Síntoma**: Errores por falta de espacio

**Solución**:
```sh
# Ver uso de espacio en contenedores
docker system df

# Limpiar contenedores parados
docker container prune

# Limpiar imágenes no utilizadas
docker image prune -a

# Limpiar volúmenes no utilizados
docker volume prune
```

## 📚 Referencias y Documentación

### Documentación Oficial

- [Apache Hadoop](https://hadoop.apache.org/docs/stable/)
- [Apache Hive](https://hive.apache.org/)
- [Apache Kafka](https://kafka.apache.org/documentation/)
- [Apache Airflow](https://airflow.apache.org/docs/)
- [Docker](https://docs.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

### Tutoriales y Guías

- [HDFS Commands Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/FileSystemShell.html)
- [Hive Language Manual](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)
- [Kafka Quick Start](https://kafka.apache.org/quickstart)
- [Airflow Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html)

### Recursos Adicionales

- [Hadoop: The Definitive Guide](https://www.oreilly.com/library/view/hadoop-the-definitive/9781491901687/)
- [Designing Data-Intensive Applications](https://dataintensive.net/)
- [Docker Hub - Hadoop Images](https://hub.docker.com/search?q=hadoop)
- [Confluent Kafka Images](https://hub.docker.com/u/confluentinc)

## 👥 Contribuciones

Este proyecto es parte del material docente de la Universidad de Sevilla. Para sugerencias o mejoras:

1. Abre un issue en el repositorio
2. Crea un fork y envía un pull request
3. Contacta con los profesores de la asignatura

## 📄 Licencia

Este proyecto está destinado para uso educativo en la Universidad de Sevilla.

---

**Desarrollado para la asignatura de Ingeniería de Datos: Big Data**  
Máster en Ingeniería del Software - Cloud, Datos y Gestión TI  
Escuela Técnica Superior de Ingeniería Informática  
Universidad de Sevilla

<p align="right">(<a href="#readme-top">Volver arriba</a>)</p>