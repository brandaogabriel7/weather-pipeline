from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.extract import extract_all_locations
from src.transform import transform_all_data
from src.load import load_all_data
from config.config import API_BASE_URL, LOCATIONS, DB_CONFIG


default_args = {
    "owner": "data enginnering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def extract_task(**context):
    """Airflow task for extraction."""
    raw_data = extract_all_locations(API_BASE_URL, LOCATIONS)
    context["task_instance"].xcom_push(key="raw_data", value=raw_data)
    return len(raw_data)


def transform_task(**context):
    """Airflow task for transformation."""
    raw_data = context["task_instance"].xcom_pull(task_ids="extract", key="raw_data")
    transformed = transform_all_data(raw_data)

    transformed_dict = {
        "current": transformed["current"].to_dict("records"),
        "hourly": transformed["hourly"].to_dict("records"),
    }

    context["task_instance"].xcom_push(key="transformed_data", value=transformed_dict)
    return True


def load_task(**context):
    """Airflow task for loading."""
    import pandas as pd

    transformed_dict = context["task_instance"].xcom_pull(
        task_ids="transform", key="transformed_data"
    )

    transformed_data = {
        "current": pd.DataFrame(transformed_dict["current"]),
        "hourly": pd.DataFrame(transformed_dict["hourly"]),
    }

    results = load_all_data(transformed_data, DB_CONFIG)
    return results


with DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    description="Daily weather data pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "etl"],
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=extract_task)

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
    )

    extract >> transform >> load
