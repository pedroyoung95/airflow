from airflow import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id = "dags_conn_test",
    schedule=None,
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    t1 = EmptyOperator(
        task_id = "t1"
    )
    
    t2 = EmptyOperator(
        task_id = "t2"
    )
    
    t3 = EmptyOperator(
        task_id = "t3"
    )

    t4 = EmptyOperator(
        task_id = "t4"
    )

    t5 = EmptyOperator(
        task_id = "t5"
    )

    t6 = EmptyOperator(
        task_id = "t6"
    )

    t7 = EmptyOperator(
        task_id = "t7"
    )

    t8 = EmptyOperator(
        task_id = "t8"
    )

    #parallel task lists
    task_list_1 = [t2, t3]
    task_list_2 = [t4,t7]
    

    t1>>task_list_1>>t4
    t5>>t4
    task_list_2>>t6>>t8

