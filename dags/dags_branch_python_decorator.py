import pendulum
from airflow.decorators import task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG(
    dag_id = "dags_branch_python_decorator",
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    schedule='0 1 * * *',
    catchup = False
) as dag :
    @task.branch(task_id='python_branch_decorator')
    def select_random() :
        import random
        item_lst = ['A','B','C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item in ['B','C']:
            return ['task_b', 'task_c']

    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    ) 

    select_random() >> [task_a, task_b, task_c]