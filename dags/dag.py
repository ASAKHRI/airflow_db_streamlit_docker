from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime
import subprocess
import os
import glob

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 1),
    'retries': 1
}

def check_csv_file():
    # Cherche n'importe quel fichier .csv dans le dossier data
    files = glob.glob('/opt/airflow/data/*.csv')
    return len(files) > 0  # True si au moins un fichier CSV existe

with DAG(
    dag_id='etl_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    check_csv_file_sensor = PythonSensor(
        task_id='check_csv_file',
        python_callable=check_csv_file,
        poke_interval=10,
        timeout=300,
        mode='poke'
    )

    def run_etl_script():
        result = subprocess.run(['python', '/opt/airflow/etl/etl.py'], text=True, capture_output=True)
        if result.returncode != 0:
            raise Exception(f"ETL script failed with error: {result.stderr}")
        print("ETL script executed successfully.")

    etl_task = PythonOperator(
        task_id='run_etl_script',
        python_callable=run_etl_script
    )

    check_csv_file_sensor >> etl_task
