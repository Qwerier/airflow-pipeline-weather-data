import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from callables import export_postgres_to_csv

with DAG(
    dag_id = "postgres_to_csv",
    schedule_interval="@once",
    start_date=datetime.datetime(2023,5,10),
    catchup=True
) as dag:
    export_task = PythonOperator(
        task_id='export_postgres_to_csv',
        python_callable=export_postgres_to_csv
    )

    export_task



