# Initialize (not for production just test)

Need to create an admin user in the PSQL DB : 

`docker compose up airflow-init`

# How to run

To run the docker-compose, please follow the instructions from the official [Airflow documentions](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

**To access airflow** : 

- username : **airflow**
- password : **airflow**


# The files 

My DAG is located in `dags/weatherdata.py`. containning the tasks : 

1. `extract_data` : Fetch the [onpenweathermap](https://openweathermap.org/api/geocoding-api)

2. `transform_data` : Format the fetched data.

3. `load_data` : Export the formatted data to the postgres DB.


The `notebool.ipynb` calls the postgres DB and read the datas to show them in a plotly graph.