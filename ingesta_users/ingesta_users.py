# Ingesta de tabla de Usuariios
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
logs_file = "logs_output/ingesta_users.log"
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
    users.drop(columns=['password'], inplace=True)

    # Guardar como json
    users_file = 'users.json'
    users.to_json(users_file, orient='records', lines=True)

    # Guardar el json en bucket S3
    s3_users_path = f'users/users{i}.json'
    try:
        s3.upload_file(users_file, bucket_name, s3_users_path)
    except Exception as e:
        logger.error(f'No se pudo subir la página {i} a un bucket de S3. Excepción: {str(e)}')


    i += 1
    print("Processed page No ", i)
logger.info(f'Se procesaron {i} páginas.')
exit_program(False)