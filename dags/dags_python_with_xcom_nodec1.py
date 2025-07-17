from airflow.models.dag import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_xcom_nodec1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 7, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def xcom_push1(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key='result1', value='value_1')
        ti.xcom_push(key='result2', value=[1,2,3])

    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key='result1', value='value_2')
        ti.xcom_push(key='result2', value=[1,2,3,4])
    
    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(key='result1', task_ids='python_xcom_push_task1')
        value2 = ti.xcom_pull(key='result2', task_ids='python_xcom_push_task1')
        print(value1)
        print(value2)

    python_xcom_push_task1 = PythonOperator(
        task_id='python_xcom_push_task1',
        python_callable=xcom_push1
    )

    python_xcom_push_task2 = PythonOperator(
        task_id='python_xcom_push_task2',
        python_callable=xcom_push2
    )

    python_xcom_pull_task = PythonOperator(
        task_id='python_xcom_pull_task',
        python_callable=xcom_pull
    )
    

    python_xcom_push_task1 >> python_xcom_push_task2 >> python_xcom_pull_task