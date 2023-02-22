import os
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from prefect import task, flow
from prefect_aws.s3 import S3Bucket
from prefect.tasks import task_input_hash


@task(retries=3, cache_key_fn=task_input_hash)
def get_data(dataset_file: str, dataset_url: str, color: str) -> Path:
    """Download taxi data from web as parquet file"""
    os.system(f"wget {dataset_url} -P data/{color}")
    path = Path(f"data/{color}/{dataset_file}")
    return path


@task(log_prints=True)
def convert_data(path: Path) -> pd.DataFrame:
    """Convert data to DataFrame and print some info"""
    df = pq.read_table(path)
    df = df.to_pandas()
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df
    

@task()
def write_ycs(path: Path) -> None:
    """Upload local parquet file to Yandex Cloud Storage"""
    ycs_block = S3Bucket.load("ycs")
    ycs_block.upload_from_path(path, path)


@flow()
def etl_web_to_ycs() -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}.parquet"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"

    path = get_data(dataset_file, dataset_url, color)
    df = convert_data(path)
    write_ycs(path)
    

if __name__ == "__main__":
    etl_web_to_ycs()
