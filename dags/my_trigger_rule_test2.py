from datetime import datetime

import random
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from time import sleep
from airflow.utils.edgemodifier import Label


with DAG(
        dag_id="my_trigger_rule_test2",
        schedule=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["trigger_rule"]
) as dag:
    all_done_flag = EmptyOperator(task_id="all_done_flag", trigger_rule="all_done")
    one_success_flag = EmptyOperator(task_id="one_success_flag", trigger_rule="one_success")

    @task
    def task1():
        sleep(5)

    @task
    def task2():
        if random.random() *100 > 30:
            raise Exception("Error")

    @task
    def task3():  pass

    @task
    def task4():  pass

    t1 = task1()
    t2 = task2()
    t3 = task3()
    t4 = task4()

    [t1, t2, t3] >> all_done_flag
    [t1, t2, t3] >> one_success_flag
    [all_done_flag,one_success_flag] >> t4
