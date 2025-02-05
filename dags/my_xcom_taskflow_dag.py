from airflow.decorators import dag, task

import pendulum
import requests
import json

url = 'https://api.covidtracking.com/v1/'
state = 'us'


@dag('my_xcom_taskflow_dag', schedule_interval='@daily', start_date=pendulum.datetime(2022, 10, 3, tz="UTC"),
     catchup=False, tags=["xcom"])
def taskflow():
    @task
    def get_testing_increase(state):
        """
        Gets totalTestResultsIncrease field from Covid API for given state and returns value
        """
        res = requests.get(url + '{0}/current.json'.format(state))
        return {'testing_increase': json.loads(res.text)[0]['totalTestResultsIncrease']}

    @task
    def analyze_testing_increases(testing_increase: int):
        """
        Evaluates testing increase results
        """
        print('Testing increases for {0}:'.format(state), testing_increase)
        # run some analysis here

    analyze_testing_increases(get_testing_increase(state))

#    cmd = BashOperator(
#        task_id='bash_executor',
#        bash_command='{{ ti.xcom_pull(task_ids="get_testing_increase", key="testing_increase") }}',
#    )
#def print_xcom(**kwargs):
#    ti = kwargs['ti']
#    print('The value is: {}'.format(
#        ti.xcom_pull(task_ids='get_testing_increase')
#    ))



dag = taskflow()
