from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


with DAG(
    dag_id='my_dynamic_task_dag',
    schedule=None,
    start_date=pendulum.datetime(2022, 10, 3, tz="UTC"),
    catchup=False,
    tags=['dynamic_task'],
) as dag:

    @task
    def dynamic_task(x: int, y: int):
        print(x, y)
        return  x+y

    dynamic_task.partial(y=10).expand(x=[1, 2, 3, 4])