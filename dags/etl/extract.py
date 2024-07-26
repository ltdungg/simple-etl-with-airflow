import wget
from time import time
import pandas as pd

def _extract():
    t_start = time()
    
    print("Extracting...")
    URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"
    parquet_name = URL.split('/')[len(URL.split('/')) - 1]
    wget.download(URL, out='/opt/airflow/data')
    
    converter = pd.read_parquet(f"/opt/airflow/data/{parquet_name}")
    converter.to_csv('/opt/airflow/data/output.csv')
    
    t_end = time()
    
    print(f'Total extracting time: {(t_end - t_start):.3f} second...')
    
    
    
    
    