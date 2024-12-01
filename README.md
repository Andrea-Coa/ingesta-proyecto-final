# Data Science: Proyecto Final Cloud computing

## Recursos a crear

- Usar `crear_mv_ingesta.yaml` para crear una máquina virtual por cada stage (dev, test, prod).

- Crear buckets para:
    - Almacenar datos ingeridos (un bucket por stage)
    - Almacenar resultados de consultas Athena

## Ingesta

```bash
git clone https://github.com/Andrea-Coa/ingesta-proyecto-final.git
```
Crear un directorio logs_output en `/HOME` dentro de cada MV  de Ciencia de Datos. Este será un volumen para guardar los logs de las ejecuciones de los contenedores.

Hay un archivo `.env` para cada stage con sus variables respectivas. Dentro modificar los nombres de los buckets a los que se crearon en [Recursos](#recursos-a-crear).

```bash
cd ingesta-proyecto-final
docker compose --env-file .env.<stage> up -d
```

Por ejemplo, para el stage dev se colocaría:

```bash
cd ingesta-proyecto-final
docker compose --env-file .env.dev up -d
```

Esta ejecución debería ingerir los datos de las tablas DynamoDB y guardarlos en distintas carpetas del bucket de S3 especificado.

## Transformación

Crear base de datos en Glue por cada stage:

- dev-pizzicato
- test-pizzicato
- prod-pizzicato

Para crear las tablas, dentro de `create_glue_tables.py` especificar el rol (LabRole). Luego ejecutar por cada stage:

```bash
STAGE=<stage-name> python create_glue_tables.py
```

Por ejemplo, para el stage de producción, ejecutar:

```bash
STAGE=prod python create_glue_tables.py
```


Esperar unos minutos ya que los crawlers tardan un rato en ejecutarse.

### ETL
Crear un contenedor para ETL:

```bash
cd etl
docker build -t etl .
docker run --rm --name etl-container -v $HOME/.aws/credentials:/root/.aws/credentials:ro etl
```
Crear contenedor de MySQL:

```bash
docker run --rm  --name pizzicato-summary-mysql-c -e MYSQL_ROOT_PASSWORD=123 -p 8081:3306 -d mysql:8.0
```

Dentro crear las tablas:

```sql
DROP DATABASE IF EXISTS pizzicato_summary_db;
CREATE DATABASE pizzicato_summary_db CHARSET utf8mb4;

USE pizzicato_summary_db;

CREATE TABLE song_count_by_genre (
    genre TEXT,
    freq INT
);
CREATE TABLE song_count_by_year (
    release_year INT,
    freq INT
);
```