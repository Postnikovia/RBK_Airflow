import logging

import pendulum

from airflow import DAG
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

dag_description = """Description for the DAG
The DAG checks a file existing in a folder
"""


with DAG(
        dag_id='my_file_sensor',
        timetable=CronTriggerTimetable('*/15 * * * *', timezone='UTC'),  # At 01:00 on Wednesday
        start_date=pendulum.datetime(2022, 9, 25, 7, 16, 0, tz="UTC"),
        catchup=False,
        tags=['example'],
        description=dag_description
) as dag:
    file_sensor = FileSensor(task_id="file_sensor",
                             filepath="test_file.csv",
                             timeout=10,
                             poke_interval=6,
                             fs_conn_id= "my_fs_conn",
                             soft_fail=True,
                             )

    some_work = EmptyOperator(task_id=f'some_work')
    file_sensor >> some_work
