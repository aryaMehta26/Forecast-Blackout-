from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='ELT_Outage_Weather_Join',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'elt'],
) as dag:

    run_dbt = BashOperator(
        task_id='run_dbt_transformations',
        bash_command=(
            'export PATH="$PATH:/home/airflow/.local/bin" && '
            'cd "$DBT_PROJECT_DIR" && '
            'dbt run --profiles-dir . --models stg_outage_data stg_weather_data int_outage_weather_joined && '
            'dbt test --profiles-dir .'
        ),
        env={
            'DBT_PROJECT_DIR': '/opt/airflow/dags/forecastblackout',
            'DBT_ACCOUNT': 'sfedu02-ksb65579',
            'DBT_USER': Variable.get("snowflake_username"),
            'DBT_PASSWORD': Variable.get("snowflake_password"),
            'DBT_DATABASE': 'USER_DB_CHIPMUNK',
            'DBT_SCHEMA': 'analytics',
            'DBT_ROLE': 'TRAINING_ROLE',
            'DBT_WAREHOUSE': 'CHIPMUNK_QUERY_WH',
        },
    )
