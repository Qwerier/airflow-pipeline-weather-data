import csv
import requests
from io import StringIO
from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery as bq
from google.cloud import storage
import tempfile
import os


def fetch_data():
    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    api_key = "a8128ae4051b8ac16605afa8507021c3"
    city = "Tirana"
    url = f"{base_url}appid={api_key}&q={city}&units=metric"

    response = requests.get(url).json()
    return {
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


def write_to_gcs(**context):
    weather_rec = fetch_data()
    bucket_name = 'us-central1-airflow-gruener-63b748cc-bucket'
    timestamp = dt.now().strftime("%Y%m%d%H%M%S")
    file_path = f'weather_{timestamp}.csv'

    csv_data = StringIO()
    gcs_hook = GCSHook()

    fieldnames = list(weather_rec.keys())

    csv_writer = csv.DictWriter(csv_data, fieldnames=fieldnames)
    csv_writer.writeheader()
    csv_writer.writerow(weather_rec)

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(csv_data.getvalue().encode())

    gcs_hook.upload(bucket_name, file_path, temp_file.name, mime_type="text/csv")
    context['ti'].xcom_push(key='file', value=file_path)
    context['ti'].xcom_push(key="bucket", value=bucket_name)

    os.remove(temp_file.name)


def write_to_bigquery(**context):
    file_path = context['ti'].xcom_pull(key="file")
    bucket_name = context['ti'].xcom_pull(key="bucket")

    client = bq.Client()

    table_id = "data-max-internship.internship_dataset_us.gruener-weather-data"

    job_config = bq.LoadJobConfig(
        schema=[
            bq.SchemaField("sunrise", "DATE"),
            bq.SchemaField("sunset", "DATE"),
            bq.SchemaField("temperature", "FLOAT"),
            bq.SchemaField("pressure", "INTEGER"),
            bq.SchemaField("humidity", "INTEGER"),
            bq.SchemaField("clouds", "INTEGER"),
            bq.SchemaField("wind_speed", "FLOAT"),
            bq.SchemaField("rain", "FLOAT"),
            bq.SchemaField("snow", "FLOAT")
        ],
        skip_leading_rows=1,
        source_format=bq.SourceFormat.CSV
    )
    uri = f"gs://{bucket_name}/{file_path}"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )
    load_job.result()

    move_file(bucket_name, file_path)


def move_file(bucket_name, file_path):
    dest_folder = 'weather'
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    source_blob = bucket.blob(file_path)

    dest_blob_path = f"{dest_folder}/{file_path}"
    bucket.rename_blob(source_blob, dest_blob_path)


with DAG(
        start_date=dt(2023,5,12),
        schedule_interval="@hourly",
        dag_id="cloud",
        catchup=False
) as dag:
    to_gcs = PythonOperator(
        task_id="write_to_gcs",
        python_callable=write_to_gcs
    )
    gcs_to_bq = PythonOperator(
        task_id="from_gcs_to_bq",
        python_callable=write_to_bigquery
    )

    to_gcs >> gcs_to_bq
