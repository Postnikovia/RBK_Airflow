from airflow import DAG
import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def hello():
    print('Hello world')


config = Variable.get("hello", deserialize_json=True)
dag = DAG('hello', description='hello',
          schedule_interval=config['time_start'],
          start_date=datetime.datetime(2021, 11, 7), catchup=False)
start_step = DummyOperator(task_id="start_step", dag=dag)
hello_step = PythonOperator(task_id="hello_step",
                            python_callable=hello,
                            dag=dag)
start_step >> hello_step

