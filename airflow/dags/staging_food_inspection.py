import pandas as pd
from datetime import timedelta, datetime
from sqlalchemy import create_engine

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def stagingNycFoodInspections(user, password, host, port, db, table_name):
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{int(port)}/{db}')
        print(f'Connected to {engine.url.database}')
    
        url = 'https://data.cityofchicago.org/api/views/4ijn-s7e5/rows.tsv?accessType=DOWNLOAD&bom=true'
        data = pd.read_csv(url, sep='\t', low_memory=False)
        print('Row Count = ', len(data))

        # Replacing nulls since explode operation eliminates them
        data["Violations"].fillna("NONE", inplace = True)

        data['Violations'] = data['Violations'].str.split('|')
        data_explode = data.explode('Violations')
        print('Row Count after explode = ', len(data_explode))

        data_explode.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
        print('Table Created')

        # Append to tableend')
        print(f'Inserted to {table_name}')
        data_explode.to_sql(name=table_name, con=engine, if_exists='append')

    except Exception as e:
        print(e)

# initializing the default arguments
default_args = {
    'owner': 'Skudli',
    'start_date': days_ago(0),
    'retries': 0
}

# Instantiate a DAG object
staging_food_inspection_dag = DAG(
    dag_id='staging_food_inspection',
    default_args=default_args,
    description='Staging NYC Food Inspection',
    schedule_interval='0 0 * * *', 
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
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

    with TaskGroup(group_id="dbt_etl") as dbt_etl:
            dbt_project_path_export = BashOperator(
            task_id="dbt_project_path_export",
            bash_command='export DBT_PROFILES_DIR=/opt/dbt_food_inspection'
            )

            dbt_dep_install = BashOperator(
            task_id="dbt_dep_install",
            bash_command='cd /opt/dbt_food_inspection;  dbt deps'
            )

            dbt_run_build = BashOperator(
            task_id="dbt_run_build",
            bash_command="cd /opt/dbt_food_inspection; dbt build --target pg_dev"
            )

            dbt_project_path_export >> dbt_dep_install >> dbt_run_build

    # Set the order of execution of tasks. 
    ingest_staging_food_inspection_task >> dbt_etl