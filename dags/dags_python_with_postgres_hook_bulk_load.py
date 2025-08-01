import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
        dag_id='dags_python_with_postgres_hook_bulk_load',
        start_date=pendulum.datetime(2025, 7, 1, tz='Asia/Seoul'),
        schedule='0 7 * * *',
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(tbl_nm, file_nm)
    
    insrt_postgres = PythonOperator(
        task_id = 'insrt_postgres',
        python_callable = insrt_postgres,
        op_kwargs = {'postgres_conn_id' : 'conn-db-postgres-custom',
                     'tbl_nm' : 'culturaleventinfo_bulk1',
                     'file_nm' : '/opt/airflow/files/culturalEventInfo/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/culturalEventInfo.csv'
                    }
    )
