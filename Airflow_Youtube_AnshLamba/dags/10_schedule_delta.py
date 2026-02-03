from airflow.decorators import dag, task
from pendulum import datetime, duration
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
    dag_id="delta_schedule_dag",
    start_date=datetime(2026, 1, 26, tz="America/Halifax"),
    end_date=datetime(2026, 1, 31, tz="America/Halifax"),
    schedule=DeltaTriggerTimetable(
        duration(days=3)
    ),
    catchup=True,  # change to False if not testing backfill
    is_paused_upon_creation=False,
)
def delta_schedule_dag():

    @task
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")

    @task
    def third_task():
        print("This is the third task")

    first_task() >> second_task() >> third_task()


delta_schedule_dag()
