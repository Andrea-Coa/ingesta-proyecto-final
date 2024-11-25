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
# from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.transform import TransformationInjector

print(os.environ['TABLE_NAME'])
table_name = os.environ['TABLE_NAME']

s3 = boto3.client('s3')
client = boto3.client('dynamodb')
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
    if original_last_evaluated_key:
        page['LastEvaluatedKey'] = original_last_evaluated_key # reset al original

    # Cada ítem de la página
    items = page['Items'] 

    # Tabla principal
    songs = pd.DataFrame.from_records(items) 

    # La columna data es un diccionario. La transformamos en otra tabla.
    # Queremos mantener el id de la canción para poder hacer joins posteriormente.
    song_data = pd.json_normalize(songs['data']).join(songs['track_id']) 

    # Guardar como csv
    song_file = f'songs.csv'
    song_data_file = f'song_data.csv'
    songs.to_csv(song_file, index = False)
    song_data.to_csv(song_data_file, index = False)

    # Guardar el csv en un bucket S3
    bucketname = 'my-test-bucket-acc'
    
    # Songs en un folder
    s3_songs_path = f'songs/songs{i}.csv'
    s3.upload_file(song_file, bucketname, s3_songs_path)

    # Song_data en otro folder
    s3_song_data_path = f'song_data/song_data{i}.csv'
    s3.upload_file(song_data_file, bucketname, s3_song_data_path)

    i += 1
    print("Processed page No ", i)
print("Finished")