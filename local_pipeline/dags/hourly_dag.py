from airflow import DAG
from datetime import datetime as dt
import requests
import pandas as pd
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine


def fetch_data():
    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    api_key = "a8128ae4051b8ac16605afa8507021c3"
    city = "Tirana"
    url = f"{base_url}appid={api_key}&q={city}&units=metric"

    response = requests.get(url).json()
    record = {
        "sunrise": dt.fromtimestamp(response["sys"]["sunrise"]),
        "sunset": dt.fromtimestamp(response["sys"]["sunset"]),
        "temperature": response["main"]["temp"],
        "pressure": response["main"]["pressure"],
        "humidity": response["main"]["humidity"],
        "clouds": response["clouds"]["all"],
        "wind_speed": response["wind"]["speed"],
        "rain": response.get("rain", {}).get("1h", 0),
        "snow": response.get("snow", {}).get("1h", 0)
    }

    url = f'postgresql://airflow:airflow@postgres:5432/airflow'
    engine = create_engine(url)
    pd.DataFrame(record).to_sql("daily", engine, if_exists="append", index=False)


with DAG(
        dag_id="hourly_dag",
        start_date=dt(2023, 5, 14),
        schedule_interval="@hourly",
        catchup=False
) as dag:
    task1 = PythonOperator(
        task_id="task1",
        python_callable=fetch_data
    )

    task1
