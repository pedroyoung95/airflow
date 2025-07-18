import pendulum
from airflow.providers.standard.operators.python import BaseBranchOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

with DAG(
    dag_id = "dags_base_branch_operator",
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    schedule='0 1 * * *',
    catchup = False
) as dag :
    class CustomBranchOperator(BaseBranchOperator) : #BaseBranchOperator를 상속받은 클래스로 branch 구현
        def choose_branch(self, context) : #BaseBranchOperator를 상속받으면 반드시 overriding 해야 하는 메소드
            import random
            print(context)
            item_lst = ['A', 'B', 'C']
            selected_item = random.choice(item_lst)
            if selected_item == 'A' :
                return 'task_a'
            elif selected_item in ['B', 'C'] :
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

    custom_branch_operator = CustomBranchOperator(task_id = 'python_branch_task')    
    custom_branch_operator >> [task_a, task_b, task_c]