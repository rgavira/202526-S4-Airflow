<a name="readme-top"></a>
# Ingeniería de Datos: Big Data - S4-Airflow

Este repositorio contiene el entorno de desarrollo para **Apache Airflow** utilizado en la **Sesión 4** de la asignatura **Ingeniería de Datos: Big Data** del **Máster en Ingeniería del Software - Cloud, Datos y Gestión TI** de la Escuela Técnica Superior de Ingeniería Informática de la Universidad de Sevilla.

## 🚀 Características  

- 📌 Entorno basado en Docker: Fácil despliegue y configuración de los servicios.
- 📁 Soporte para Hadoop HDFS: Almacenamiento distribuido de archivos.
- 🔍 Hive: Consultas SQL sobre datos distribuidos en HDFS.
- 📡 Kafka: Procesamiento en tiempo real con productores y consumidores.
- 🚀 Apache Airflow: Orquestación de flujos de datos con DAGs personalizados.
- 🔄 Integración completa: Airflow puede interactuar con HDFS, Hive y Kafka.

## 📂 Estructura del Proyecto  

```
📂 S4-Airflow/
├── 📄 docker-compose.yml           # Configuración de servicios Docker
├── 📄 dockerfile                   # Imagen base de Hadoop
├── 📄 Dockerfile.airflow          # Imagen personalizada de Airflow
├── 📄 Dockerfile.kafka-connect    # Imagen de Kafka Connect
├── 📄 README.md                    # Documentación del proyecto
│
├── 📂 airflow/                     # Configuración de Airflow
│   ├── airflow-webserver.pid
│   ├── airflow.cfg
│   └── webserver_config.py
│
├── 📂 hadoop_config/               # Configuración de Hadoop
│   ├── capacity-scheduler.xml
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── log4j.properties
│   ├── mapred-site.xml
│   └── yarn-site.xml
│
├── 📂 hive/                        # Configuración de Hive
│   └── 📂 conf/
│       ├── core-site.xml
│       ├── hive-log4j2.properties
│       └── hive-site.xml
│
├── 📂 mysql/                       # Scripts de inicialización de MySQL
│   └── init.sql
│
├── 📂 scripts/                     # Scripts de inicialización
│   ├── init-datanode.sh
│   ├── start-hdfs.sh
│   ├── start-nodemanager.sh
│   └── start-yarn.sh
│
└── 📂 src/                         # Código fuente y DAGs
    ├── example_hdfs.py
    └── 📂 dags_demo/
        ├── dag_hdfs.py
        ├── dag_hello.py
        ├── dag_hive.py
        ├── dag_kafka.py
        └── dag_random.py
```

## 🛠️ Requisitos  

- **Docker** y **Docker Compose** instalados en el sistema.  
- **RAM**: Mínimo 8GB (recomendado 16GB+ para entornos completos).
- **Espacio en disco**: Al menos 20GB libres para contenedores y datos.

## ⚡ Instalación y Uso  

1️⃣ Clona este repositorio:  
```sh
git clone https://github.com/josemarialuna/hdfs-docker-cluster.git
cd S4-Airflow
```

2️⃣ Inicia el clúster completo con Docker Compose:  
```sh
docker-compose up -d
```

3️⃣ Verifica que los contenedores están en ejecución:  
```sh
docker ps
```

4️⃣ Accede a la interfaz web de Airflow:  
```
http://localhost:8081
Usuario: admin
Contraseña: admin
```

5️⃣ Accede a otras interfaces:
- **HDFS NameNode**: http://localhost:9870
- **YARN ResourceManager**: http://localhost:8088
- **Hive Server**: puerto 10000
- **Kafka UI**: http://localhost:8080

## 📌 Comandos Útiles  

### HDFS

🔹 Acceder al contenedor del NameNode:
```sh
docker exec -it namenode bash
```

🔹 Listar los archivos en HDFS:  
```sh
hdfs dfs -ls /
```

🔹 Subir un archivo a HDFS:  
```sh
hdfs dfs -put archivo.txt /ruta/destino/
```

🔹 Descargar un archivo de HDFS:  
```sh
hdfs dfs -get /ruta/origen/archivo.txt .
```

🔹 Ver el estado del clúster:  
```sh
hdfs dfsadmin -report
```

🔹 Salir manualmente del Safe Mode: 
```sh
hdfs dfsadmin -safemode leave
```

### Airflow

🔹 Acceder al contenedor de Airflow:
```sh
docker exec -it airflow bash
```

🔹 Listar DAGs disponibles:
```sh
airflow dags list
```

🔹 Ejecutar un DAG manualmente:
```sh
airflow dags trigger <dag_id>
```

## 📝 Notas  

- El sistema está configurado para un entorno de desarrollo, no para producción.  
- Airflow utiliza **SequentialExecutor** con SQLite para simplicidad.
- Las conexiones a HDFS, Hive y Kafka están preconfiguradas en Airflow.

## ❓ FAQ  

**El namenode me da un error de unexpected end of file**

Verifica caracteres ocultos en el fichero. Ejecuta:
```sh
cat -A start-hdfs.sh
```
Si ves ^M al final de las líneas, el archivo tiene formato Windows y debes convertirlo.
```sh
sed -i 's/\r$//' start-hdfs.sh
```

## 📖 Referencias  

- [Documentación oficial de Hadoop](https://hadoop.apache.org/docs/stable/)  
- [Documentación oficial de Apache Airflow](https://airflow.apache.org/docs/)
- [Docker Hub - Hadoop Images](https://hub.docker.com/)  

<p align="right">(<a href="#readme-top">Volver arriba</a>)</p>