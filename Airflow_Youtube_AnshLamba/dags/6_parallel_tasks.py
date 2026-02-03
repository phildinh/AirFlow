from airflow.decorators import dag, task

@dag(
    dag_id="parallel_dag"
)
def parallel_dag():

    @task.python
    def extract_task(**kwargs):
        ti = kwargs["ti"]
        print("Extracting Data...")

        extracted_data_dict = {
            "api_extracted_data": [1, 2, 3],
            "db_extracted_data": [4, 5, 6],
            "s3_extracted_data": [7, 8, 9],
        }

        ti.xcom_push(
            key="return_value",
            value=extracted_data_dict
        )

    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs["ti"]

        api_data = ti.xcom_pull(
            task_ids="extract_task",
            key="return_value"
        )["api_extracted_data"]

        print(f"Transforming API Data... {api_data}")
        ti.xcom_push(key="return_value", value=[i * 10 for i in api_data])

    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs["ti"]

        db_data = ti.xcom_pull(
            task_ids="extract_task",
            key="return_value"
        )["db_extracted_data"]

        print(f"Transforming DB Data... {db_data}")
        ti.xcom_push(key="return_value", value=[i * 10 for i in db_data])

    @task.python
    def transform_task_s3(**kwargs):
        ti = kwargs["ti"]

        s3_data = ti.xcom_pull(
            task_ids="extract_task",
            key="return_value"
        )["s3_extracted_data"]

        print(f"Transforming S3 Data... {s3_data}")
        ti.xcom_push(key="return_value", value=[i * 10 for i in s3_data])

    @task.bash
    def load_task(**kwargs):
        ti = kwargs["ti"]

        api_data = ti.xcom_pull(
            task_ids="transform_task_api",
            key="return_value"
        )
        db_data = ti.xcom_pull(
            task_ids="transform_task_db",
            key="return_value"
        )
        s3_data = ti.xcom_pull(
            task_ids="transform_task_s3",
            key="return_value"
        )

        return f"echo 'Loaded data: {api_data}, {db_data}, {s3_data}'"

    extract = extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()
    load = load_task()

    extract >> [transform_api, transform_db, transform_s3] >> load


parallel_dag()
