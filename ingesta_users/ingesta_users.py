# Ingesta de tabla de Usuariios
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

operation_parameters = {
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

    # Datos como df
    users = pd.DataFrame.from_records(items)

    # Guardar como csv
    users_file = 'users.csv'
    users.to_csv(users_file, index = False)

    # Guardar el csv en bucket S3
    s3_users_path = f'users/users{i}.csv'
    s3.upload_file(users_file, bucket_name, s3_users_path)

    i += 1
    print("Processed page No ", i)
print("Finished")