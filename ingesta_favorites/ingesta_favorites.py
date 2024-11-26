# Ingesta de tabla de Favorites
# -----------------------------
#   - Conectarse a DynamoDB
#   - Scan toda la tabla por páginas
#   - Por cada página:
#       - guardar datos como pandas DataFrame
#       - guardar DataFrames resultantes como csv
#       - subir a bucket s3.

import copy
import pandas as pd
import boto3
import os
# from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.transform import TransformationInjector

table_name = os.environ['TABLE_NAME']
bucket_name = os.environ['BUCKET_NAME']

s3 = boto3.client('s3')
client = boto3.client('dynamodb', region_name='us-east-1')
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
    favorites = pd.DataFrame.from_records(items)

    # Guardar como csv
    favorites_file = f'favorites.csv'
    favorites.to_csv(favorites_file, index = False)

    # Guardar en folder de bucket S3
    s3_favorites_file = f'favorites/favorites{i}.csv'
    s3.upload_file(favorites_file, bucket_name, s3_favorites_file)

    i+=1
    print("Processed page No ", i)
print("Finished")