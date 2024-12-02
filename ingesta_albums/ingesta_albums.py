# Ingesta de tabla de Favorites
# -----------------------------
#   - Conectarse a DynamoDB
#   - Scan toda la tabla por páginas
#   - Por cada página:
#       - guardar datos como pandas DataFrame
#       - guardar DataFrames resultantes como json
#       - subir a bucket s3.

import copy
import pandas as pd
import boto3
import os
import sys
from loguru import logger
# from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.transform import TransformationInjector

# logging configuration
logs_file = "logs_output/ingesta_albums.log"
id = 'ingesta-albums'
logger.add(logs_file)

def critical( message):
    logger.critical(f"{id} - {message}")
def info(message):
    logger.info(f'{id} - {message}')
def error(message):
    logger.error(f'{id} - {message}')
def warning(message):
    logger.warning(f'{id} - {message}')

def exit_program(early_exit=False):
    if early_exit:
        warning('Saliendo del programa antes de la ejecución debido a un error previo.')
        sys.exit(1)
    else:
        info('Programa terminado exitosamente.')



# aws resource names
table_name = os.environ.get('TABLE_NAME')
bucket_name = os.environ.get('BUCKET_NAME')

if not table_name:
    critical('No se encontró el nombre de la tabla.')
    exit_program(True)
if not bucket_name:
    critical('No se encontró el nombre del bucket de S3.')
    exit_program(True)


# conectarse a  s3 y dynamodb
try:
    s3 = boto3.client('s3')
    info('Conexión a S3 exitosa.')
except Exception as e:
    critical(f'No fue posible conectarse a S3. Excepción: {e}')
    exit_program(True)
    
try:
    client = boto3.client('dynamodb', region_name='us-east-1')
    info('Conexión a DynamoDB exitosa')
except Exception as e:
    critical(f'No fue posible conectarse a DynamoDB. Excepción: {e}')
    exit_program(True)


# tools
paginator = client.get_paginator('scan')
service_model = client._service_model.operation_model('Scan')
trans = TransformationInjector(deserializer = TypeDeserializer())

operation_parameters = { # datos a ingerir
    'TableName': table_name,
}
i = 0   # inicializar contador

for page in paginator.paginate(**operation_parameters):
    # guardar LastEvaluatedKey
    original_last_evaluated_key = ""
    if 'LastEvaluatedKey' in page:
        original_last_evaluated_key =  copy.copy(page['LastEvaluatedKey'])
    print(original_last_evaluated_key)

    # transformar
    trans.inject_attribute_value_output(page, service_model)
    if original_last_evaluated_key:
        page['LastEvaluatedKey'] = original_last_evaluated_key # reset al original

    # Cada ítem de la página
    items = page['Items'] 

    # Tabla
    albums = pd.DataFrame.from_records(items)
    albums['date'] = albums['date#genre'].str.split('#').str[0]
    albums.drop(columns=['date#genre'], inplace=True)

    # Guardar como json
    albums_file = f'albums.json'
    albums.to_json(albums_file, orient='records', lines=True)

    # Guardar en folder de bucket S3
    s3_albums_file = f'albums/albums{i}.json'
    try:
        s3.upload_file(albums_file, bucket_name, s3_albums_file)
    except Exception as e:
        error(f'No se pudo subir la página {i} a un bucket de S3. Excepción: {str(e)}')

    i+=1
    print("Processed page No ", i)
info(f'Se procesaron {i} páginas.')
exit_program(False)