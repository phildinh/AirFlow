from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning"]
)
def user_processing():

    # create table
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users(
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP
        );
        """
    )

    # sensor
    @task.sensor(poke_interval=10, timeout=60)
    def is_api_available():
        import requests
        r = requests.get("https://jsonplaceholder.typicode.com/users/1")
        if r.status_code == 200:
            return PokeReturnValue(is_done=True, xcom_value=r.json())
        return PokeReturnValue(is_done=False)

    # extract
    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["name"].split()[0],
            "lastname": fake_user["name"].split()[-1],
            "email": fake_user["email"],
            "created_at": datetime.now().to_datetime_string(),
        }

    # write CSV
    @task
    def process_user(user_info):
        import csv
        path = "/tmp/user_info.csv"

        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)

        return path

    # copy into postgres
    @task
    def store_user(path):
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename=path
        )

    # pipeline wiring
    fake_user = is_api_available()
    user_info = extract_user(fake_user)
    csv_path = process_user(user_info)
    load = store_user(csv_path)

    create_table >> fake_user >> user_info >> csv_path >> load


user_processing()
