from airflow.sdk import DAG, task
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_python_with_trigger_rule_eg2",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    @task.branch(task_id = 'branching')
    def random_branch() :
        import random
        item_lst = ['A', 'B', 'C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A' :
            return 'task_a'
        elif selected_item == 'B' :
            return 'task_b'
        elif selected_item == 'C' :
            return 'task_c'
    
    task_a = BashOperator(
        task_id = 'task_a',
        bash_command = 'echo upstream1'
    )

    @task(task_id = 'task_b')
    def task_b() :
        print('정상 처리')
    
    @task(task_id = 'task_c')
    def task_c() :
        print('정상 처리')

    @task(task_id = 'task_d', trigger_rule = 'none_skipped')
    def task_d() :
        print('정상 처리')
    
    random_branch() >> [task_a, task_b(), task_c()] >> task_d()
    
    


