from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import json
import unicodedata
import re
import os

# ========== CONFIG ==========
CONFIG_FILE = "/opt/airflow/dags/config.json"

with open(CONFIG_FILE, "r") as f:
    cfg = json.load(f)

API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("OPENWEATHER_API_KEY not set in environment variables")
CITIES = cfg["cities"]

# ========== HELPERS ==========

def safe_task_id(city: str) -> str:
    """
    Convert city names to safe task_id strings:
    - lowercase
    - remove accents
    - replace spaces and non-alphanumeric with _
    """
    city_ascii = (
        unicodedata.normalize("NFKD", city)
        .encode("ascii", "ignore")
        .decode("utf-8")
    )
    return re.sub(r"[^a-zA-Z0-9]+", "_", city_ascii.lower()).strip("_")


def extract_weather(city: str, **context):
    """Extract weather data from OpenWeather API"""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    data = response.json()
    context["ti"].xcom_push(key=f"raw_{city}", value=data)


def load_raw_weather(city: str, **context):
    """Load raw weather JSON into Postgres"""
    data = context["ti"].xcom_pull(key=f"raw_{city}")
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_weather (
            city TEXT,
            raw JSONB,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("INSERT INTO raw_weather (city, raw) VALUES (%s, %s)", (city, json.dumps(data)))
    conn.commit()
    cur.close()
    conn.close()


def transform_and_load(city: str, **context):
    """Transform JSON and load structured data"""
    data = context["ti"].xcom_pull(key=f"raw_{city}")

    if not data or "main" not in data or "weather" not in data:
        raise ValueError(f"Missing expected keys in API response for {city}: {data}")

    transformed = {
        "city": data.get("name", city),
        "temp": data["main"].get("temp"),
        "feels_like": data["main"].get("feels_like"),
        "weather": data["weather"][0].get("description"),
        "dt": datetime.utcfromtimestamp(data["dt"]).isoformat()
    }

    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            city TEXT,
            temp FLOAT,
            feels_like FLOAT,
            weather TEXT,
            dt TIMESTAMP,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        INSERT INTO weather (city, temp, feels_like, weather, dt)
        VALUES (%s, %s, %s, %s, %s)
    """, (transformed["city"], transformed["temp"], transformed["feels_like"],
          transformed["weather"], transformed["dt"]))
    conn.commit()
    cur.close()
    conn.close()

# ========== DAG DEFINITION ==========

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_etl",
    default_args=default_args,
    description="ETL weather data into Postgres",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    for city in CITIES:
        city_task_id = safe_task_id(city)

        extract = PythonOperator(
            task_id=f"extract_{city_task_id}",
            python_callable=extract_weather,
            op_args=[city],
            provide_context=True,
        )

        load_raw = PythonOperator(
            task_id=f"load_raw_{city_task_id}",
            python_callable=load_raw_weather,
            op_args=[city],
            provide_context=True,
        )

        transform = PythonOperator(
            task_id=f"transform_{city_task_id}",
            python_callable=transform_and_load,
            op_args=[city],
            provide_context=True,
        )

        extract >> load_raw >> transform
