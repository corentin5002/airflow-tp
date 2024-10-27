import requests as rq
import pandas as pd
from datetime import datetime
import sqlalchemy as sql
import json

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def get_weather_data(**kwargs):

    key= "10298ad4ad011275f9bf76b178d85730"
    city = "Clermont-Ferrand"  

    # find city lat lon via their name
    cityInfoAPI = f"http://api.openweathermap.org/data/2.5/weather?q={city},FR&appid={key}"
    
    response = rq.get(cityInfoAPI)

    if response.status_code != 200:
        return None


    kwargs['ti'].xcom_push(key='raw_data', value=response.json())

def tranform_weather_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='raw_data', task_ids='extract_data')
    # Traitement

    df = {
        'Date' :        [datetime.fromtimestamp(data['dt'])],
        'City' :        [data['name']],
        'Lat' :         [data['coord']['lat']],
        'Lon' :         [data['coord']['lon']],
        'Temp' :        [data['main']['temp']       - 273.15],
        'Feels_like' :  [data['main']['feels_like'] - 273.15],
    }

    # Dict to pandas dataframe
    df = pd.DataFrame(df)

    kwargs['ti'].xcom_push(key='transformed_data', value=df)


def export_to_postgres(**kwargs):
    newData = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_data')

    # Export 
    username = 'airflow'
    password = 'airflow'
    database = 'airflow'
    container = 'postgres'


    connexion_string = f'postgresql+psycopg2://{username}:{password}@{container}/{database}'

    sqlEngine = sql.create_engine(connexion_string)
    dbProstgres = sqlEngine.connect()

    tableName = "weather_data"

    try: 
        frame = newData.to_sql(tableName, dbProstgres, if_exists='append', index=False)

    except ValueError as vx:
        print(vx)

    dbProstgres.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}


with DAG(
    'weather_dag', 
    default_args=default_args, 
    schedule_interval='*/5 * * * *', 
    catchup=False
    ) as weatherDag : 
    
    extractData = PythonOperator(
        task_id='extract_data',
        python_callable=get_weather_data,
        op_args=[],
        dag=weatherDag
    )

    transformData = PythonOperator(
        task_id='transform_data',
        python_callable=tranform_weather_data,
        op_kwargs={"data": "{{ ti.xcom_pull(task_ids='extract_data') }}"},
        dag=weatherDag
    )

    export = PythonOperator(
        task_id='load_data',
        python_callable=export_to_postgres,
        op_kwargs={"newData": "{{ ti.xcom_pull(task_ids='transform_data') }}"},
        dag=weatherDag
    )

    extractData >> transformData >> export

