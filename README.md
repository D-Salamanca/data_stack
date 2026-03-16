# Lakehouse Data Pipeline – Grupo 5

Este proyecto implementa un **pipeline de datos tipo Lakehouse** utilizando tecnologías modernas de ingeniería de datos.
El objetivo es construir un flujo completo de ingestión, almacenamiento, catalogación, transferencia y consulta analítica de datos.

El pipeline procesa un dataset de viajes de taxi en **formato Parquet**, pasando por diferentes etapas hasta quedar disponible para análisis en **ClickHouse**.

---

# Arquitectura del Pipeline

El flujo completo de datos es el siguiente:

```
HTTP Dataset
     │
     ▼
MinIO (Raw Data Lake)
     │
     ▼
Apache Iceberg
     │
     ▼
Nessie Catalog
     │
     ├──────────────► Azure Data Lake Storage
     │
     ▼
ClickHouse (Analytics)
```

Este diseño sigue principios de arquitectura **Lakehouse**, donde:

* **MinIO** actúa como Data Lake compatible con S3
* **Iceberg** gestiona tablas analíticas
* **Nessie** versiona metadatos de datos
* **ClickHouse** permite consultas analíticas de alto rendimiento

---

# Infraestructura

Toda la infraestructura se ejecuta mediante **Docker Compose**, que levanta los siguientes servicios:

| Servicio       | Descripción                                 |
| -------------- | ------------------------------------------- |
| **MinIO**      | Almacenamiento de objetos compatible con S3 |
| **Nessie**     | Catálogo versionado para tablas Iceberg     |
| **ClickHouse** | Motor analítico columnar                    |
| **JupyterLab** | Entorno para ejecutar scripts y notebooks   |

Para iniciar el entorno:

```bash
docker compose up -d
docker ps
```

Acceso a las interfaces:

| Servicio        | URL                                            |
| --------------- | ---------------------------------------------- |
| JupyterLab      | [http://localhost:8888](http://localhost:8888) |
| MinIO Console   | [http://localhost:9001](http://localhost:9001) |
| ClickHouse HTTP | [http://localhost:8123](http://localhost:8123) |

---

# Estructura del Proyecto

```
data_stack/
│
├── docker-compose.yml
├── requirements.txt
├── .env
│
├── scripts/
│   ├── 01_http_to_minio.py
│   └── 04_iceberg_to_clickhouse.py
│
├── notebooks/
│   ├── 02_parquet_to_iceberg.ipynb
│   └── 03_minio_to_azure.ipynb
│
└── .dlt/
    └── secrets.toml
```

Cada etapa del pipeline está implementada como **script o notebook independiente**, lo que facilita reproducibilidad y pruebas.

---

# 1. Ingesta de datos (HTTP → MinIO)

En la primera etapa se descarga un dataset en formato **Parquet** desde una fuente HTTP pública y se almacena en el Data Lake.

MinIO actúa como la **zona RAW**, donde los datos se guardan sin transformación.

Ejecución del script:

```bash
docker exec -it fhbd-jupyter python /home/jovyan/scripts/01_http_to_minio.py
```

El script realiza:

1. Descarga del archivo Parquet desde la fuente HTTP
2. Conexión a MinIO utilizando la API compatible con S3
3. Carga del archivo en el bucket `raw`

Ubicación final del archivo:

```
s3://raw/nyc/yellow_tripdata_2025-01.parquet
```

Esta etapa representa la **ingesta inicial del pipeline**.

---

# 2. Conversión de Parquet a Iceberg

En esta etapa se transforma el archivo Parquet almacenado en MinIO en una **tabla Iceberg**, utilizando **Nessie como catálogo de metadatos**.

Se utilizan las siguientes librerías:

* PyArrow
* PyIceberg
* boto3

El proceso consiste en:

1. Leer el archivo Parquet desde el bucket `raw`
2. Inferir el esquema del dataset
3. Crear una tabla Iceberg
4. Registrar la tabla en el catálogo Nessie
5. Almacenar los archivos de datos en el bucket `iceberg`

Resultado:

```
iceberg/nyc/yellow_tripdata
```

Iceberg gestiona:

* esquema de la tabla
* snapshots
* metadatos
* versionado de datos

---

# 3. Transferencia a Azure Data Lake

En esta etapa se transfieren los archivos Parquet de la tabla Iceberg hacia **Azure Data Lake Storage**.

Para esto se utiliza **DLT (Data Load Tool)**, que permite construir pipelines de transferencia entre sistemas de almacenamiento.

Configuración en:

```
.dlt/secrets.toml
```

El pipeline:

1. Lee archivos Parquet desde el bucket `iceberg` en MinIO
2. Utiliza credenciales S3 compatibles
3. Transfiere los archivos a Azure Data Lake Storage

Destino final:

```
Azure Data Lake Storage
GRUPO_5/
```

Se valida la transferencia verificando los archivos cargados y leyendo los datos con PyArrow.

---

# 4. Carga analítica en ClickHouse

La última etapa del pipeline carga los datos desde Iceberg hacia **ClickHouse** para permitir consultas analíticas.

El script principal:

```
scripts/04_iceberg_to_clickhouse.py
```

El proceso realiza:

1. Conexión al catálogo Iceberg mediante Nessie
2. Lectura de la tabla Iceberg en batches
3. Transformación de datos a formato compatible
4. Inserción en ClickHouse mediante **DLT**

Base de datos destino:

```
lakehouse
```

Tabla creada:

```
yellow_tripdata
```

---

# Validación de resultados

Una vez cargados los datos en ClickHouse, se pueden ejecutar las siguientes consultas:

Ver tablas disponibles:

```sql
SHOW TABLES FROM lakehouse;
```

Contar registros:

```sql
SELECT count() FROM lakehouse.yellow_tripdata;
```

Resultado esperado:

```
3475226 registros
```

También se crean tablas de metadatos de DLT:

```
_dlt_loads
_dlt_pipeline_state
_dlt_version
dlt_sentinel_table
```

---

# Tecnologías utilizadas

El proyecto integra varias herramientas del ecosistema moderno de datos:

* Docker
* MinIO
* Apache Iceberg
* Project Nessie
* ClickHouse
* PyArrow
* PyIceberg
* boto3
* DLT
* Azure Data Lake Storage

---

# Conclusión

Este proyecto demuestra la construcción de un pipeline de datos basado en arquitectura **Lakehouse**, separando las responsabilidades de:

* almacenamiento
* catalogación
* versionado
* transferencia
* consulta analítica

La solución permite mantener un flujo de datos **reproducible, escalable y desacoplado**, facilitando la evolución del sistema y la integración con diferentes motores de análisis.
