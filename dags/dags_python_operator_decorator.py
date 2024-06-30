from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.decorators import task
import random

with DAG(
    dag_id="dags_python_operator_decorator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task() #task_id 값 없으면 함수 명이 task_id명
    def select_fruit():
        fruit = ['APPLE', 'BANANA', 'ORANGE','AVOCADO']
        rand_int = random.randint(0,len(fruit))
        print(fruit[rand_int])

    py_t1 = select_fruit()
     