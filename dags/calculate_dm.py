from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
import pandas as pd

def save_to_db():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
    data = pd.read_csv("/opt/airflow/dags/data.csv")
#    cursor =postgres_hook.cursor()  устарело
    for row in data.iloc:
        print(row.size_name, row.min_square)
        postgres_hook.run("INSERT INTO bd_shops.market_size (size_name,min_square) VALUES (%s,%s)",
                       parameters =(row.size_name, int(row.min_square)))

# Параметры запуска ДАГа
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}

# Запускаемый скрипт
SQL_QUERY =  """
insert into bd_shop_dm.dm_sales
(
select p.revizion_date as rev_date, shop_id, sum(p.count * p.price_sale_out) as sales_volume
from bd_shops.product p
group by p.revizion_date, shop_id
order by p.revizion_date
)
"""

# Создание объкта ДАГа
@dag(
    "calcuate_dm",
    start_date=datetime(2024, 10, 1),
    max_active_runs=3,
    schedule="@daily",
    default_args=default_args,
    catchup=False,
)

# Функция для выстраивания пайплайна (в ней мы задаем задачи и последовательность их запуска)
def call_snowflake_sprocs():
    # запуск sql запроса
    calculate_dm = SQLExecuteQueryOperator(
        task_id="calculate_dm", conn_id="postgres_conn_id", sql=SQL_QUERY
    )
    # запись справочника в БД (Не важно в каком порядке объявлены задачи важно в каком порядке они выстроены в зависимостях)
    python_task = PythonOperator(
        task_id='my_python_task',
        python_callable=save_to_db
    )
    # шаг  пустышка (финальный шаг)
    end_step = EmptyOperator(task_id="end_step")

    python_task >> calculate_dm >> end_step


call_snowflake_sprocs()