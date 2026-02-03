from airflow.decorators import dag, task
from pendulum import datetime
import os

DATA_PATH = "/opt/airflow/logs/data/data_extract.txt"

@dag(
    dag_id="fetch_data",
    schedule="@daily",
    start_date=datetime(2026, 3, 1, tz="America/Halifax"),
    catchup=False
)
def fetch_data_dag():

    @task(outlets=[data_asset])
    def fetch_data():
        os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)

        with open(DATA_PATH, "w") as f:
            f.write(f"Data fetched on {datetime.now('America/Halifax')}")

        print(f"Data written to {DATA_PATH}")

    fetch_data()


fetch_data_dag()
