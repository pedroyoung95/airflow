from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
import random


with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def select_fruit():
        fruit = ['APPLE', 'BANANA', 'ORANGE','AVOCADO']
        rand_int = random.randint(0,len(fruit))
        print(fruit[rand_int])
    
    py_t1 = PythonOperator(
        task_id = 'py_t1',
        python_callable=select_fruit
    )

    py_t1
     