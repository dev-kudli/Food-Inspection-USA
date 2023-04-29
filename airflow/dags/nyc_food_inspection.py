import pandas as pd
from datetime import timedelta, datetime
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def stagingNycFoodInspections(user, password, host, port, db, table_name):
    try:
        url = 'https://data.cityofchicago.org/api/views/4ijn-s7e5/rows.tsv?accessType=DOWNLOAD&bom=true'
        data = pd.read_csv(url, sep='\t', low_memory=False)
        print('Row Count = ', len(data))

        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        print(f'Connected to {engine.url.database}')

        # Append to table
        data.to_sql(name=table_name, con=engine, if_exists='append')
        print(f'Inserted to {table_name}')

    except Exception as e:
        print(e)

# initializing the default arguments
default_args = {
    'owner': 'Skudli',
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
staging_nyc_food_inspection_dag = DAG('staging_nyc_food_inspection',
    default_args=default_args,
    description='Staging NYC Food Inspection',
    schedule_interval='@daily', 
    catchup=False,
    tags=['nyc, food, inspection']
)

with staging_nyc_food_inspection_dag:
    # Creating first task
    ingest_staging_nyc_food_inspection_task = PythonOperator(
        task_id='staging_nyc_food_inspection', 
        python_callable=stagingNycFoodInspections, 
        op_kwargs=dict(
            user = 'root',
            password = 'root',
            host = 'pgdatabase',
            port = 5432,
            db = 'nyc_food_inspection',
            table_name = 'staging_chicago'
        ),
        dag=staging_nyc_food_inspection_dag
    )

    # Creating second task
    end_task = DummyOperator(task_id='end_task', dag=staging_nyc_food_inspection_dag)

    # Set the order of execution of tasks. 
    ingest_staging_nyc_food_inspection_task >> end_task