from datetime import datetime, timezone

import requests
from pymongo import MongoClient
from airflow.decorators import dag, task
import os

FRANKFURTER_API_URL = os.getenv("FRANKFURTER_API_URL", "https://api.frankfurter.dev/v2/rates")
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@mongo:27017/?authSource=admin"


@dag(
    dag_id="currency_etl_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
)
def currency_exchange_dag():
    @task(task_id="extract_exchange_rates") 
    def extract(**context):
        res = requests.get(FRANKFURTER_API_URL, params={"base": "EUR", "quotes": "USD,GBP,JPY"}, timeout=30)
        res.raise_for_status()
        payload = {
            "rows": res.json(),
            "retrieved_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        }
        context["ti"].xcom_push(key="raw_rates", value=payload)
        return {"extracted": len(payload["rows"])}

    @task(task_id="transform_exchange_rates")
    def transform(**context):
        data = context["ti"].xcom_pull(task_ids="extract_exchange_rates", key="raw_rates")
        records = [
            {
                "base_currency": r["base"],
                "target_currency": r["quote"],
                "exchange_rate": r["rate"],
                "rate_date": r["date"],
                "source": "Frankfurter API",
                "retrieved_at": data["retrieved_at"],
            }
            for r in data["rows"]
        ]
        context["ti"].xcom_push(key="clean_rates", value=records)
        return {"transformed": len(records)}

    @task(task_id="load_to_mongodb")
    def load(**context):
        records = context["ti"].xcom_pull(task_ids="transform_exchange_rates", key="clean_rates")
        client = MongoClient(MONGO_URI)
        client["forex"]["exchange_rates"].insert_many(records)
        client.close()
        return {"loaded": len(records)}

    extract() >> transform() >> load() # type: ignore

currency_exchange_dag()
