import pandas as pd
from time import time
from sqlalchemy import create_engine


def _transform_and_load():
    
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')

    df_iter = pd.read_csv('/opt/airflow/data/output.csv', iterator=True, chunksize=100000)

    df = next(df_iter)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df.head(0).to_sql('taxi_trips', engine, if_exists='replace')
    
    df.to_sql('taxi_trips', engine, if_exists='append')

    while True:
        t_start = time()
        df = next(df_iter)

        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        df.to_sql('taxi_trips', engine, if_exists='append')

        t_end = time()
        print("insert another chunk.... took %.3f second" % (t_end - t_start))