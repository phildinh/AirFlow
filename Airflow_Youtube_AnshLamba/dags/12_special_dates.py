from airflow.decorators import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

special_dates = EventsTimetable(
    event_dates=[
        datetime(2026, 3, 1, tz="America/Halifax"),
        datetime(2026, 3, 15, tz="America/Halifax"),
        datetime(2026, 3, 26, tz="America/Halifax"),
        datetime(2026, 3, 30, tz="America/Halifax"),
    ]
)

@dag(
    dag_id="special_dates_dag",
    schedule=special_dates,
    start_date=datetime(2026, 1, 1, tz="America/Halifax"),
    end_date=datetime(2026, 3, 31, tz="America/Halifax"),
    catchup=True
)
def special_dates_dag():

    @task.python
    def special_event_task(**kwargs):
        logical_date = kwargs["logical_date"]
        print(f"Running task for special event on {logical_date}")

    special_event_task()


special_dates_dag()
