from airflow.models.dag import DAG
import pendulum
#from airflow.decorators import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_bash_python_with_xcom",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def python_push_xcom(**kwargs):
        result_dic = {'status':'Good', 'data':[1,2,3],'options_cnt':100}
        kwargs['ti'].xcom_push(value=result_dic)
    
    def python_pull_xcom(**kwargs):
        status_value = kwargs['ti'].xcom_pull(key='bash_pushed')
        return_value = kwargs['ti'].xcom_pull(task_ids='bash_push')
        print('status_value' + str(status_value))
        print('return_value' + return_value)
    
    bash_pull = BashOperator(
        task_id='bash_pull',
        env={'STATUS':'{{ ti.xcom_pull(task_ids="python_push")["status"] }}',
             'DATA':'{{ ti.xcom_pull(task_ids="python_push")["data"] }}',
             'OPTIONS_CNT':'{{ ti.xcom_pull(task_ids="python_push")["options_cnt"] }}'
            },
        bash_command='echo $STATUS && echo $DATA && echo $OPTIONS_CNT'
    )

    python_push = PythonOperator(
        task_id='python_push',
        python_callable=python_push_xcom
    )    

    bash_push = BashOperator(
        task_id = 'bash_push',
        bash_command='echo PUSH_START '
                     '{{ ti.xcom_push(key="bash_pushed", value=200) }} && '
                     'echo PUSH_COMPLETE '
    )

    python_pull = PythonOperator(
        task_id='python_pull',
        python_callable=python_pull_xcom
    )    

    
    python_push >> bash_pull >> bash_push >> python_pull