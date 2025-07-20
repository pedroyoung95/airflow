import pendulum
from airflow.providers.http.operators.http import HttpOperator
from airflow.sdk import DAG, task

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:
    '''서울도서관 새로 들어온 도서정보'''
    tb_new_books_info = HttpOperator(
        task_id = 'tb_new_books_info',
        http_conn_id = 'openapi.seoul.go.kr',
        endpoint = '{{var.value.apikeyapikey_openapi_seoul_go_kr}}/json/SeoulLibNewArrivalInfo/1/100/',
        method = 'GET',
        headers = {'Content-Type' : 'application/json',
                   'charset' : 'utf-8',
                   'Accept' : '*/*'
                   }
    )

    @task(task_id = 'data_transfer')
    def data_transfer(**kwargs) :
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids = 'tb_new_books_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))
    
    tb_new_books_info >> data_transfer()