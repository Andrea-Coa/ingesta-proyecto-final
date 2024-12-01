# Ingesta de tabla de Artistas
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
logs_file = "logs_output/ingesta_songs.log"
logger.remove(0)
logger.add(sys.stderr, format='{time:MMMM D, YYYY > HH:mm:ss} | {level} | {message} | ingesta-songs-c')
def exit_program(early_exit=False):
    if early_exit:
        logger.warning('Saliendo del programa antes de la ejecución debido a un error previo.')
        sys.exit(1)
    else:
        logger.info('Programa terminado exitosamente.')


# aws resource names
table_name = os.environ.get('TABLE_NAME')
bucket_name = os.environ.get('BUCKET_NAME')

if not table_name:
    logger.critical('No se encontró el nombre de la tabla.')
    exit_program(True)
if not bucket_name:
    logger.critical('No se encontró el nombre del bucket de S3.')
    exit_program(True)


# conectarse a s3 y dynamo
try:
    s3 = boto3.client('s3')
    logger.info('Conexión a S3 exitosa.')
except Exception as e:
    logger.critical(f'No fue posible conectarse a S3. Excepción: {e}')
    exit_program(True)
    
try:
    client = boto3.client('dynamodb', region_name='us-east-1')
    logger.info('Conexión a DynamoDB exitosa')
except Exception as e:
    logger.critical(f'No fue posible conectarse a DynamoDB. Excepción: {e}')
    exit_program(True)


# tools
paginator = client.get_paginator('scan')
service_model = client._service_model.operation_model('Scan')
trans = TransformationInjector(deserializer = TypeDeserializer())

operation_parameters = { # datos a ingerir
    'TableName': table_name,
}
i = 0   # inicializar contador

# iterar por cada página
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
    artists = pd.DataFrame.from_records(items)

    # Guardar como json
    artists_file = f'artists.json'
    artists.to_json(artists_file, orient='records', lines=True)

    # Guardar en folder de bucket S3
    s3_artists_file = f'artists/artists{i}.json'
    try:
        s3.upload_file(artists_file, bucket_name, s3_artists_file)
    except Exception as e:
        logger.error(f'No se pudo subir la página {i} a un bucket de S3. Excepción: {str(e)}')

    i+=1

logger.info(f'Se procesaron {i} páginas.')
exit_program(False)