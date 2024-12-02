import os
import sys
import boto3
from loguru import logger
import time

role = "arn:aws:iam::483433628337:role/LabRole" # LabRole

# logger setup
# logs_file = 'logs/glue_logs/logs.log'
logger.remove(0)
logger.add(sys.stderr)
def exit_program(early_exit = False):
    if early_exit:
        logger.warning('Aborting program due to previuos error.')
        sys.exit(1)
    else:
        logger.info('Program ended successfully.')


# get stage
stage = os.environ.get('stage')
stage = 'prod'
if not stage:
    logger.warning('No stage was provided as environment variable. Defaulting to stage dev.')
    stage = 'dev'

bucket_name = f'{stage}-ingestion-results-acc'

# connect to glue client
try:
    glue = boto3.client('glue')
    logger.info('Successfully connected to Glue.')
except Exception as e:
    logger.critical(f'Failed to connect to Glue. Error: {str(e)}')
    exit_program(early_exit=True)


# tables 
tables = ['songs', 'song_data', 'albums', 'artists', 'users', 'favorites']

# classifiers to read csv files correctly
# classifiers = []

# try:
#     for table in tables:
#             classifier_name =  f'{stage}_{table}_json'
#             glue.create_classifier(JsonClassifier= {
#                 'Name': classifier_name,
#                 'JsonPath': '$'
#             })
#     classifiers.append(classifier_name)
#     logger.info('Created all classifiers.')
# except Exception as e:
#     logger.critical(f'Failed classifier creation. Error: {str(e)}')
#     exit_program(early_exit=True)


time.sleep(10)


# create crawler
crawler_name = f'{stage}-pizzicato-crawler'
s3_targets = [{'Path': f's3://{bucket_name}/{table}'} for table in tables]
database_name = f'{stage}-pizzicato-db'

try: 
    response = glue.create_crawler(Name = crawler_name,
                                   Role = role,
                                   DatabaseName = database_name,
                                   Targets = {
                                            'S3Targets': [{'Path': f's3://{bucket_name}/{table}'} for table in tables]
                                    })
    logger.info(str(response))
    logger.info('Created all crawlers successfully')
except Exception as e:
    logger.critical(f'Failed to create crawlers. Error: {str(e)}')
    exit_program(early_exit=True)


# start crawler
try:
    glue.start_crawler(Name=crawler_name)
    logger.info('Crawler successfully started.')
except Exception as e:
    logger.critical(f'Failed to start crawlers. Error: {str(e)}')
    exit_program(early_exit=True)

exit_program(early_exit=False)