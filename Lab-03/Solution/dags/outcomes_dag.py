
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

from etl_scripts.transform import transform_data
SOURCE_URL = 'https://data.austintexas.gov/resource/9t4d-g238.csv'
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
CSV_TARGET_DIR = AIRFLOW_HOME + '/data/downloads'
CSV_TARGET_FILE = CSV_TARGET_DIR + '/outcomes.csv'
with DAG(
    dag_id="outcomes_dag",
    start_date=datetime(2023, 11, 1),
    schedule_interval='@daily'

) as dag:
    extract = BashOperator(
        task_id="extract",
        bash_command=f"curl --create-dirs -o {CSV_TARGET_FILE} {SOURCE_URL}",
    )
    trasform = PythonOperator(
        task_id="trasform",
        python_callable=transform_data,
        op_kwargs={
            'source_csv': CSV_TARGET_FILE
        }
    )
    extract >> trasform
