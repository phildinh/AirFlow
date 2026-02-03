from airflow.decorators import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
    dag_id="cron_schedule_dag",
    start_date=datetime(2026, 1, 26, tz="America/Halifax"),
    end_date=datetime(2026, 1, 31, tz="America/Halifax"),
    schedule=CronTriggerTimetable(
        "0 16 * * MON-FRI",
        timezone="America/Halifax"
    ),
    catchup=True,  # change to False if not testing backfill
    is_paused_upon_creation=False,
)
def cron_schedule_dag():

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


cron_schedule_dag()
