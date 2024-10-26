import pprint as pp
import requests as rq
import pandas as pd
import datetime

# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from datetime import datetime

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 10, 1),
#     'retries': 1,
# }


# dag = DAG('my_dag', default_args=default_args, schedule_interval='@daily')


key= "10298ad4ad011275f9bf76b178d85730"


def requestcity(key, city):

# find city lat lon via their name
    cityInfoAPI = f"http://api.openweathermap.org/data/2.5/weather?q={city},FR&appid={key}"
    
    response = rq.get(cityInfoAPI)

    if response.status_code != 200:
        return None

    data = response.json()
    pp.pprint(data)

    df = {
        'Date' : [datetime.datetime.fromtimestamp(data['dt'])],
        'City' : [data['name']],
        'Coord' : [[data['coord']['lat'], data['coord']['lon']]],
        'Temp' : [data['main']['temp'] - 273.15],
        'Feels_like' : [data['main']['feels_like'] - 273.15],
    }

    # Dict to pandas dataframe
    df = pd.DataFrame(df)
    return df


weatherData = pd.DataFrame({
    'Date' : [],
    'City' : [],
    'Coord' : [],
    'Temp' : [],
    'Feels_like' : [],
}
)


newWeatherData = requestcity(key, "Clermont-Ferrand")

# Concat new data to the old one
weatherData = pd.concat([weatherData, newWeatherData], ignore_index=True)

print(weatherData)

# start = DummyOperator(task_id='start', dag=dag)
# end = DummyOperator(task_id='end', dag=dag)

# start >> end