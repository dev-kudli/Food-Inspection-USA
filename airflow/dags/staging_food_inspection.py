import pandas as pd
from datetime import timedelta, datetime
from sqlalchemy import create_engine

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def stagingNycFoodInspections(user, password, host, port, db, table_name):
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{int(port)}/{db}')
        print(f'Connected to {engine.url.database}')
    
        url = 'https://data.cityofchicago.org/api/views/4ijn-s7e5/rows.tsv?accessType=DOWNLOAD&bom=true'
        data = pd.read_csv(url, sep='\t', low_memory=False)
        print('Row Count = ', len(data))

        data['Violations'] = data['Violations'].str.split('|')
        data_explode = data.explode('Violations')
        print('Row Count after explode = ', len(data_explode))

        data_explode.head(0).to_sql(name=table_name, con=engine)
        print('Table Created')

        # Append to tableend')
        print(f'Inserted to {table_name}')
        data_explode.to_sql(name=table_name, con=engine, if_exists='append')

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
staging_food_inspection_dag = DAG('staging_food_inspection',
    default_args=default_args,
    description='Staging NYC Food Inspection',
    schedule_interval='@daily', 
    catchup=False,
    tags=['nyc, food, inspection']
)

with staging_food_inspection_dag:
    # Creating first task
    ingest_staging_food_inspection_task = PythonOperator(
        task_id='staging_food_inspection', 
        python_callable=stagingNycFoodInspections, 
        op_kwargs=dict(
            user = Variable.get('USER'),
            password = Variable.get('PASSWORD'),
            host = Variable.get('HOST'),
            port = Variable.get('PORT'),
            db = f"{Variable.get('DB')}_{Variable.get('ENV')}",
            table_name = Variable.get('TABLE_NAME')
        ),
        dag=staging_food_inspection_dag
    )

    # Creating second task
    end_task = DummyOperator(task_id='end_task', dag=staging_food_inspection_dag)

    # Set the order of execution of tasks. 
    ingest_staging_food_inspection_task >> end_task