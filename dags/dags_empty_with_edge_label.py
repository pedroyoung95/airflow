import pendulum
from airflow.sdk import DAG, Label
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_empty_with_edge_label",
    schedule=None,
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    empty_1 = EmptyOperator(
        task_id = 'empty_1'
    )
    
    empty_2 = EmptyOperator(
        task_id = 'empty_2'
    )

    empty_1 >> Label('1과 2사이') >> empty_2

    empty_3 = EmptyOperator(
        task_id = 'empty_3'
    )

    empty_4 = EmptyOperator(
        task_id = 'empty_4'
    )

    empty_5 = EmptyOperator(
        task_id = 'empty_5'
    )

    empty_6 = EmptyOperator(
        task_id = 'empty_6'
    )

    empty_2 >> Label('Start Branch') >> [empty_3, empty_4, empty_5] >> Label('End Branch') >> empty_6
    #task flow 내 task 사이에 Label을 추가하면 해당 연결 선에 Label이 붙음