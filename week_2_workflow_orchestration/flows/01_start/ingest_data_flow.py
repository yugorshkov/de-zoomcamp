import os
import pandas as pd
import pyarrow.parquet as pq
from prefect import task, flow
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash)
def get_data(url):
    file_name = url.rsplit('/', maxsplit=1)[-1]

    os.system(f"wget {url}")

    if file_name.endswith('.parquet'):
        df = pq.read_table(file_name)
        df = df.to_pandas()
    elif file_name.endswith('.csv'):
        df = pd.read_csv(file_name)

    return df

@task()
def transform_data(df):
    print(f"missing passenger count: {(df['passenger_count'] == 0).sum()}")
    df = df[df['passenger_count'] != 0]
    return df

@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
    database_block = SqlAlchemyConnector.load("postgres-connector")
    with database_block.get_connection(begin=False) as engine:
        df.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

@flow(name="Ingest Flow")
def main_flow(table_name: str):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

    raw_data = get_data(url)
    data = transform_data(raw_data)
    ingest_data(table_name, data)

if __name__ == '__main__':
    main_flow("yellow_taxi_trips")
