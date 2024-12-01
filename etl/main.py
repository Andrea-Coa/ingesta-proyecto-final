import boto3
import time
import pandas as pd

client = boto3.client('athena', region_name='us-east-1')

# VARIABLES

database = 'dev2-pizzicato' # database in glue
bucket_name = 'etl-athena-output-acc'
folder = 'local-execution'
s3_path = f's3://{bucket_name}/{folder}/'
tables = ['country_summary', 
          'artist_summary', 
          'album_summary', 
          'genre_summary', 
          'year_summary']


import mysql.connector
from mysql.connector import Error

host_name = '54.175.207.80'
port_number = '8081'
user_name = 'root'
password = 'password'
database_name = 'pizzicato_summary_db' # mysql database

# QUERIES

# country
q1 = """
select a.country, song_count, artist_count from (
        select artists.country, count(*) as song_count
        from artists join songs on artists.artist_id = songs.artist_id
        group by artists.country
    ) as a join (
        select country, count(artist_id) as artist_count
        from artists
        group by country
    ) as b on a.country = b.country;
"""

# artist
q2 = """
select artists.artist_id, artists.name as artist_name, a.favorite_count, b.song_count
from 
    artists join (
        select songs.artist_id, count(*) as favorite_count
        from songs join favorites on songs.song_uuid = favorites.song_uuid
        group by songs.artist_id 
    ) as a on artists.artist_id = a.artist_id
    join (
        select songs.artist_id, count(*) as song_count
        from songs
        group by songs.artist_id
    ) as b on artists.artist_id = b.artist_id
    order by song_count DESC;
"""
# album

q3 = """
select albums.album_uuid, albums.name as album_name, a.favorite_count, b.song_count
from 
    albums join (
        select songs.album_uuid, count(*) as favorite_count
        from songs join favorites on songs.song_uuid = favorites.song_uuid
        group by songs.album_uuid 
    ) as a on albums.album_uuid = a.album_uuid
    join (
        select songs.album_uuid, count(*) as song_count
        from songs
        group by songs.album_uuid
    ) as b on albums.album_uuid = b.album_uuid
    order by song_count DESC;
"""
# genre
q4 = """
select a.genre, a.song_count, b.album_count
from (
    select genre, count(*) as song_count
    from songs
    group by genre
) as a join (
    select genre, count(*) as album_count
    from albums
    group by genre
) as b on a.genre = b.genre;
"""

# summary
q5 = """
select year(parse_datetime(release_date, 'yyyy-MM-dd HH:mm:ss.SSSSSS'))  as release_year, count(*) as song_count
from songs
group by year(parse_datetime(release_date, 'yyyy-MM-dd HH:mm:ss.SSSSSS'))
order by release_year
"""

query_list = [q1, q2, q3, q4, q5]


# map each query to its id (assigned by aws)
queries_execution_ids = {}

# execute queries
for i, query_string in enumerate(query_list):
    query_execution = client.start_query_execution(
        QueryString = query_string,
        QueryExecutionContext = {
            'Database': database
        }, 
        ResultConfiguration = { 'OutputLocation': s3_path}
    )
    queries_execution_ids[i] = query_execution['QueryExecutionId']


# get files from s3! and write as csvs
s3 = boto3.client('s3')
files = []
for i, execution_id in queries_execution_ids.items():
    # Wait for the query to complete
    while True:
        query_status = client.get_query_execution(QueryExecutionId=execution_id)
        status = query_status['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(5)

    if status == 'SUCCEEDED':
        # Retrieve results from S3
        try:
            response = s3.get_object(
                Bucket=bucket_name,
                Key=f'{folder}/{execution_id}.csv'
            )
            csv_data = response['Body'].read()
            filename = f'athena_query{i}.csv'
            with open(filename, 'wb') as file:
                file.write(csv_data)
            files.append(filename)
        except s3.exceptions.NoSuchKey:
            print(f"Key {folder}/{execution_id}.csv does not exist in bucket {bucket_name}")
    else:
        print(f"Query {execution_id} did not succeed. Status: {status}")


mydb = mysql.connector.connect(
        host=host_name,
        port=port_number,
        user=user_name,
        password=password,
        database=database_name
)

cursor = mydb.cursor()

for i, file in enumerate(files):
    table_name = tables[i]
    df = pd.read_csv(file)

    # Convert structured array to list of tuples
    records = list(df.itertuples(index=False, name=None))

    placeholders = ', '.join(['%s'] * len(df.columns))
    columns = ', '.join(df.columns)
    sql_str = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    # Execute the query with the list of tuples
    cursor.executemany(sql_str, records)
    mydb.commit()

cursor.close()
mydb.close()