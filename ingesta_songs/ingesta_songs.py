# Ingesta de tabla de Canciones
# -----------------------------
#   - Conectarse a DynamoDB
#   - Scan toda la tabla por páginas
#   - Por cada página:
#       - guardar datos como pandas DataFrame
#       - json normalize si hay JSON anidados
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
id = 'ingesta-songs'
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
    critical('No fue posible conectarse a S3. Excepción: {e}')
    exit_program(True)
    
try:
    client = boto3.client('dynamodb', region_name='us-east-1')
    info('Conexión a DynamoDB exitosa')
except Exception as e:
    critical('No fue posible conectarse a DynamoDB. Excepción: {e}')
    exit_program(True)

# tools
paginator = client.get_paginator('scan')
service_model = client._service_model.operation_model('Scan')
trans = TransformationInjector(deserializer = TypeDeserializer())


# Los datos de DynamoDB vienen en formatos poco amigables, TransformationInjector
# los transforma en tipos de Python más manejables.

operation_parameters = { # datos a ingerir
    'TableName': table_name,
}
i = 0   # inicializar contador

# iterar por cada página
for page in paginator.paginate(**operation_parameters):
    
    # TransformationInjector modificará todo page, incluyendo LastEvaluatedKey, así 
    # que guardamos una copia del LastEvaluatedKey original para poder usarla luego
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

    # Tabla principal
    songs = pd.DataFrame.from_records(items)
    songs['release_date'] = songs['genre#release_date'].str.split('#').str[1]
    songs.drop(columns=['genre#release_date'], inplace=True)
    # songs = songs.map(normalize_strings) # quotear todo

    # La columna data es un diccionario. La transformamos en otra tabla.
    # Queremos mantener el id de la canción para poder hacer joins posteriormente.
    song_data = pd.json_normalize(songs['data']).join(songs['song_uuid']) 
    # drop columna que fue pasada a otra tabla
    songs.drop(columns = ['data'], inplace = True)

    # Guardar como json
    song_file = f'songs.json'
    song_data_file = f'song_data.json'
    songs.to_json(song_file, orient='records', lines=True)
    song_data.to_json(song_data_file,  orient='records', lines=True)

    # Guardar el json en un bucket S3    
    # Songs en un folder
    s3_songs_path = f'songs/songs{i}.json'
    try:
        s3.upload_file(song_file, bucket_name, s3_songs_path)
    except Exception as e:
        error(f'No se pudo subir la página {i} a un bucket de S3. Excepción: {str(e)}')

    # Song_data en otro folder
    s3_song_data_path = f'song_data/song_data{i}.json'
    try:
        s3.upload_file(song_data_file, bucket_name, s3_song_data_path)
    except Exception as e:
        error(f'No se pudo subir la página {i} a un bucket de S3. Excepción: {str(e)}')

    i += 1

info(f'Se procesaron {i} páginas.')
exit_program(False)