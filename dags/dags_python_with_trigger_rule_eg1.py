from airflow.sdk import DAG
import pendulum
from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators import trigger_dagrun

with DAG(
    dag_id="dags_python_with_trigger_rule_eg1",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    bash_upstream_1=BashOperator(
        task_id='bash_upstream_1',
        bash_command='echo upstream1'
    )

    @task(task_id='python_upstream_1')
    def python_upstream_1() :
        raise AirflowException('downstream_1 Exception!') #예외를 발생시켜서 task fail 만듦
    
    @task(task_id='python_upstream_2')
    def python_upstream_2() :
        print('정상 처리')

    @task(task_id='python_downstream_1', trigger_rule='all_done')
    def python_downstream_1() :
        print('정상 처리')
    
    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_upstream_1()


