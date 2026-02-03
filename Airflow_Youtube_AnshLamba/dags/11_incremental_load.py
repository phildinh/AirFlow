from airflow.decorators import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    dag_id="incremental_load_dag",
    schedule=CronDataIntervalTimetable(
        "@daily",
        timezone="America/Halifax"
    ),
    start_date=datetime(2026, 2, 1, tz="America/Halifax"),
    end_date=datetime(2026, 2, 28, tz="America/Halifax"),
    catchup=True
)
def incremental_load_dag():

    @task.python
    def incremental_data_fetch(**kwargs):
        data_interval_start = kwargs["data_interval_start"]
        data_interval_end = kwargs["data_interval_end"]

        print(
            f"Fetching data from {data_interval_start} "
            f"to {data_interval_end}"
        )

    @task.bash
    def incremental_data_process():
        return (
            "echo 'Processing incremental data from "
            "{{ data_interval_start }} to {{ data_interval_end }}'"
        )

    fetch = incremental_data_fetch()
    process = incremental_data_process()

    fetch >> process


incremental_load_dag()
