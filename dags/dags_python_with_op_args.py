from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from common.common_func import regist

with DAG(
    dag_id="dags_python_with_op_args",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    regist_t1=PythonOperator(
        task_id='regist_t1',
        python_callable=regist,
        op_args=['tyjeong', 'man', 'kr', 'gyeonggi-do']
    )

    regist_t1