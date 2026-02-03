from dag_orchestrate_1 import first_orchestrator_dag
from dag_orchestrate_2 import second_orchestrator_dag
from airflow.sdk import dag, task

@dag(
        dag_id="dag_orchestrate_parent"
)
def dag_orchestrate_parent():

    @task
    def first_orchestrator_task():
        first_orchestrator_dag

    @task
    def second_orchestrator_task():
        second_orchestrator_dag   

    first_orchestrator_task() >> second_orchestrator_task()

# Installing the dag
dag_orchestrate_parent()