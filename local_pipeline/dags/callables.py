import csv

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook


def weather_data(**context):
    lat = 41.327953
    lon = 19.819025
    APIToken = "a8128ae4051b8ac16605afa8507021c3"

    start_date = dt(2022, 5, 14)  # dt.now()
    end_date = dt(2023, 5, 8)
    end_unix = int(end_date.timestamp())
    rows = []

    while start_date < end_date:
        start_unix = int(start_date.timestamp())
        url = f"https://history.openweathermap.org/data/2.5/history/" \
              f"city?lat={lat}&lon={lon}&type=hour" \
              f"&start={start_unix}&end={end_unix}&units=metric&appid={APIToken}"

        result = requests.get(url).json()

        for record in result['list']:
            row = {'date': dt.fromtimestamp(record['dt']), 'pressure': record['main']['pressure'],
                   'humidity': record['main']['humidity'], 'clouds': record['clouds']['all'],
                   'temp': record['main']['temp'], 'wind speed': record['wind']['speed'],
                   'wind direction': record['wind']['deg'], 'weather conditions': record['weather'][0]['description'],
                   'rain': record['rain']['1h'] if 'rain' in record else 0}
            rows.append(row)
        start_date += relativedelta(weeks=1, hours=1)

    df = pd.DataFrame(rows)
    df_irrad = pd.read_csv("./data/yearly.csv", usecols=['Ghi', 'GtiFixedTilt', 'GtiTracking'])

    context['ti'].xcom_push(key='my_data_key', value=pd.concat((df, df_irrad), axis=1))


def dump(**context):
    user = 'airflow'
    password = 'airflow'
    host = 'postgres'
    port = '5432'  # The mapped port on the host machine
    database = 'airflow'

    url = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(url)

    weather_df = context['ti'].xcom_pull(key='my_data_key')
    weather_df.to_sql('historic', con=engine, if_exists="replace", index=False)


def export_postgres_to_csv():
    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')

    query = "SELECT * FROM historic"
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    output_file = "./data/historic_exported"

    with open(output_file, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow([desc[0] for desc in cursor.description])  # Write header
        csv_writer.writerows(result)  # Write rows

    cursor.close()
    connection.close()
