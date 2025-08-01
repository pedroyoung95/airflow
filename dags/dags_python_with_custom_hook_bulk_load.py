import pendulum
from hooks.custom_postgres_hook import CustomPostgresHook
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
        dag_id='dags_python_with_custom_hook_bulk_load',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        schedule='0 7 * * *',
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id = postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name = tbl_nm, file_name = file_nm, delimiter = ',', is_header = True, is_replace = False)

    insrt_postgres = PythonOperator(
        task_id = 'insrt_postgres',
        python_callable = insrt_postgres,
        op_kwargs = {'postgres_conn_id' : 'conn-db-postgres-custom',
                     'tbl_nm' : 'culturaleventinfo_bulk1',
                     'file_nm' : '/opt/airflow/files/culturalEventInfo/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/culturalEventInfo.csv'
                    }
    )

    insrt_postgres