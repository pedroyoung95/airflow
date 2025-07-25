from airflow.sdk import DAG, task, task_group, TaskGroup
import pendulum
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_task_group",
    schedule=None,
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    def inner_func(**kwargs) :
        msg = kwargs.get('msg') or ''
        print(msg)
    
    @task_group(group_id = 'first_group')
    def group_1() : 
        ''' task_group decorator를 이용한 첫 번째 그룹''' #docstring : python에서 함수의 설명을 제공 , airflow UI에서는 tooltip이라는 이름으로 나옴
        
        @task(task_id = 'inner_function1')
        def inner_func1(**kwargs) :
            print('첫 번째 TaskGroup 내 첫 번째 task입니다.')
        
        inner_function2 = PythonOperator(
            task_id = 'inner_function2',
            python_callable = inner_func,
            op_kwargs = {'msg' : '첫 번째 TaskGroup 내 두 번째 task입니다.'}
        )
    
        inner_func1() >> inner_function2


    with TaskGroup(group_id = 'second_group', tooltip = '두 번째 그룹입니다.') as group_2 :
        ''' 여기에 적은 docstring은 표시되지 않음'''
        @task(task_id = 'inner_function1') #task_id가 동일해도 소속된 group의 id가 다르기 때문에 정상 작동
        def inner_func1(**kwargs) :
            print('두 번째 TaskGroup내 첫 번째 task입니다.')
        
        inner_function2 = PythonOperator(
            task_id = 'inner_function2',
            python_callable = inner_func,
            op_kwargs = {'msg' : '두 번째 TaskGroup내 두 번째 task입니다.'}
        )

        inner_func1() >> inner_function2
    
    group_1() >> group_2