import argparse
import os
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    file_name = url.rsplit('/', maxsplit=1)[-1]

    os.system(f"wget {url}")

    if file_name.endswith('.parquet'):
        df = pq.read_table(file_name)
        df = df.to_pandas()
    elif file_name.endswith('.csv'):
        df = pd.read_csv(file_name)
    else:
        print('Неверный формат файла')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest PARQUET data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password name for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()

    main(args)
