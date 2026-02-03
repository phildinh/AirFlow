from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

@dag(
    dag_id="branch_dag",
)
def branch_dag():

    @task.python
    def extract_task(**kwargs):
        ti = kwargs["ti"]
        print("Extracting Data...")

        ti.xcom_push(
            key="return_value",
            value={
                "api_extracted_data": [1, 2, 3],
                "db_extracted_data": [4, 5, 6],
                "s3_extracted_data": [7, 8, 9],
                "weekend_flag": "false",
            }
        )

    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="extract_task", key="return_value")["api_extracted_data"]
        ti.xcom_push(key="return_value", value=[i * 10 for i in data])

    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="extract_task", key="return_value")["db_extracted_data"]
        ti.xcom_push(key="return_value", value=[i * 10 for i in data])

    @task.python
    def transform_task_s3(**kwargs):
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="extract_task", key="return_value")["s3_extracted_data"]
        ti.xcom_push(key="return_value", value=[i * 10 for i in data])

    @task.branch
    def decider_task(**kwargs):
        ti = kwargs["ti"]
        weekend_flag = ti.xcom_pull(
            task_ids="extract_task",
            key="return_value"
        )["weekend_flag"]

        return "no_load_task" if weekend_flag == "true" else "load_task"

    @task.bash(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def load_task(**kwargs):
        ti = kwargs["ti"]

        api = ti.xcom_pull(task_ids="transform_task_api", key="return_value")
        db = ti.xcom_pull(task_ids="transform_task_db", key="return_value")
        s3 = ti.xcom_pull(task_ids="transform_task_s3", key="return_value")

        return f"echo 'Loaded data: {api}, {db}, {s3}'"

    @task.bash
    def no_load_task():
        return "echo 'No loading on weekends'"

    extract = extract_task()
    api = transform_task_api()
    db = transform_task_db()
    s3 = transform_task_s3()
    decider = decider_task()
    load = load_task()
    no_load = no_load_task()

    extract >> [api, db, s3] >> decider >> [load, no_load]


branch_dag()
