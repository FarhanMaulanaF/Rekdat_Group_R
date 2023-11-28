import requests
from sqlalchemy import create_engine
from datetime import datetime
from datetime import timedelta

import pandas as pd
import psycopg2 as pg

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

dag = DAG(
    'USD_IDR_forex_exchange_dag',
    default_args=default_args,
    description='USD-IDR Forex Exchange Data',
    schedule=timedelta(days=1),
    catchup=False
)

api1 = 'https://financialmodelingprep.com/api/v3/historical-price-full/USDIDR?to=2023-11-07&apikey=PdK9QjSbplzsQY1TkBc0Pmv3XZ1YyXtd'
api2 = 'https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=USD&to_symbol=IDR&outputsize=full&apikey=5C5XMUDURIAR4OCU'

def get_data():
    r1 = requests.get(api1)
    data1 = r1.json()
    
    r2 = requests.get(api2)
    data2 = r2.json()
    
    return data1, data2

def process_data(*args, **kwargs):
    data1, data2 = kwargs['ti'].xcom_pull(task_ids='get_data')
    # Extract the relevant columns ('date' and 'close') directly

    df1 = pd.DataFrame(data1['historical'], columns=['date', 'close'])

    #Buka data2.json lalu ekstrak, load dan tampilkan
    fx_data = data2['Time Series FX (Daily)']
    df2 = pd.DataFrame.from_dict(fx_data, orient='index', columns=['4. close'])
    df2.reset_index(inplace=True)
    df2.rename(columns={'index': 'date', '4. close': 'close'}, inplace=True)
    df2['close'] = df2['close'].astype(float)
    df2 = df2.iloc[::-1]
    return df1, df2

def save_to_db(*args, **kwargs):
    df1, df2 = kwargs['ti'].xcom_pull(task_ids='process_data')
    try:
        conn = pg.connect(
            "dbname='airflow' user='postgres' host='localhost' password='admin'"
        )

        engine = create_engine('postgresql://postgres:admin@localhost:5432/airflow')

        df1.to_sql('table1', engine, if_exists='replace', index=False)

        df2.to_sql('table2', engine, if_exists='replace', index=False)

        print("Data inserted into the database.")

    except Exception as error:
        print("Error:", error)

    finally:
        conn.close()

get_data_task = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

save_to_db_task = PythonOperator(
    task_id='save_to_db',
    python_callable=save_to_db,
    provide_context=True,
    dag=dag,
)

get_data_task >> process_data_task >> save_to_db_task