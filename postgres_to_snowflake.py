import psycopg2
from dotenv import load_dotenv
import os
import snowflake.connector
import pandas as pd
import json

load_dotenv()

def extract_data_from_postrgres():
    connection = psycopg2.connect(
        dbname = os.getenv('PG_DBNAME'), 
        host = os.getenv('PG_HOST'), 
        user = os.getenv('PG_USER'), 
        password =  os.getenv('PG_PASSWORD'),
        port = os.getenv('PG_PORT')
    )

    cursor = connection.cursor()
    cursor.execute("SELECT * FROM articles;")

    return cursor.fetchall()

def load_data_to_snowflake(data, table_name):

    df = pd.DataFrame(data, columns = ['id', 'title', 'content', 'publication_date', 'source', 'description', 'urlToImage', 'url' ]) 
    df['source'] = df['source'].apply(json.dumps)
    #convert Dataframe to tuple to insert to snowflake
    rows_to_insert = [tuple(row) for row in df.to_numpy()]

    # for col, dtype in df.dtypes.items():
    #     print(f"Column: {col}, Type: {type(dtype)}")

    # print(', '.join([col for col in df.columns]))
    

    connection = snowflake.connector.connect(
        user = os.getenv('SF_USER'),
        password = os.getenv('PG_PASSWORD'),
        account = os.getenv('SF_ACCOUNT'),
        role = 'news_role',
        warehouse = os.getenv('SF_WAREHOUSE'),
        database = os.getenv('SF_DATABASE'),
        schema = os.getenv('SF_SCHEMA')
    )
    cursor = connection.cursor()

    cursor.execute(f'USE SCHEMA {os.getenv('SF_SCHEMA')}')

    cursor.execute(f"""
        CREATE OR REPLACE TABLE {table_name}(
            id NUMBER, 
            title STRING,
            content STRING, 
            publication_date TIMESTAMP_LTZ, 
            source STRING, 
            description STRING, 
            urlToImage STRING, 
            url STRING
        )
    """)

    cursor.executemany(f"""
        INSERT INTO {table_name} (id, title, content, publication_date, source, description, urlToImage, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, rows_to_insert)





if __name__ == "__main__":

    pg_data = extract_data_from_postrgres()
    load_data_to_snowflake(pg_data, 'test1')

   

