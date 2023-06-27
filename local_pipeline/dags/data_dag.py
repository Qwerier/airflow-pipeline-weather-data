from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from callables import weather_data, dump


with DAG(
        dag_id='my_dag',
        start_date=datetime(2023, 5, 8),
        catchup=True
) as dag:
    task2 = PythonOperator(
        task_id="weather",
        python_callable=weather_data
    )

    task3 = PythonOperator(
        task_id="dump",
        python_callable=dump
    )
    task2 >> task3
