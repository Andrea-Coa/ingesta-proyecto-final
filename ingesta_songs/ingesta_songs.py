# Ingesta de tabla de Canciones
# -----------------------------
#   - Conectarse a DynamoDB
#   - Scan toda la tabla por páginas
#   - Por cada página:
#       - guardar datos como pandas DataFrame
#       - json normalize si hay JSON anidados
#       - guardar DataFrames resultantes como csv
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
logger.add(logs_file, format='{time:MMMM D, YYYY > HH:mm:ss} | {level} | {message} | ingesta-songs-c')
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


# conectarse a  s3 y dynamodb
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


# Los datos de DynamoDB vienen en formatos poco amigables, TransformationInjector
# los transforma en tipos de Python más manejables.

operation_parameters = { # datos a ingerir
    'TableName': table_name,
}
i = 0   # inicializar contador


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
    songs['release_date'] = songs['genre#release_date'].str.split('#')[1]
    songs.drop(columns=['genre#release_date'], inplace=True)

    # La columna data es un diccionario. La transformamos en otra tabla.
    # Queremos mantener el id de la canción para poder hacer joins posteriormente.
    song_data = pd.json_normalize(songs['data']).join(songs['song_uuid']) 
    # drop columna que fue pasada a otra tabla
    songs.drop(columns = ['data'], inplace = True)

    # Guardar como csv
    song_file = f'songs.csv'
    song_data_file = f'song_data.csv'
    songs.to_csv(song_file, index = False)
    song_data.to_csv(song_data_file, index = False)

    # Guardar el csv en un bucket S3    
    # Songs en un folder
    s3_songs_path = f'songs/songs{i}.csv'
    try:
        s3.upload_file(song_file, bucket_name, s3_songs_path)
    except Exception as e:
        logger.error(f'No se pudo subir la página {i} a un bucket de S3. Excepción: {str(e)}')

    # Song_data en otro folder
    s3_song_data_path = f'song_data/song_data{i}.csv'
    try:
        s3.upload_file(song_data_file, bucket_name, s3_song_data_path)
    except Exception as e:
        logger.error(f'No se pudo subir la página {i} a un bucket de S3. Excepción: {str(e)}')

    i += 1

logger.info(f'Se procesaron {i} páginas.')
exit_program(False)