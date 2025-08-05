from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id = 'dags_seoul_api_culturalInfo',
    schedule = '0 7 * * *',
    start_date = pendulum.datetime(2025, 7, 1, tz = 'Asia/Seoul'),
    catchup = False
) as dag :
    '''서울시 문화행사 정보'''
    tb_culture_event_info = SeoulApiToCsvOperator(
        task_id = 'tb_culture_event_info',
        dataset_nm = 'culturalEventInfo',
        path = '/opt/airflow/files/culturalEventInfo/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name = 'culturalEventInfo.csv'
    )

    '''서울시 문화공간 정보'''
    tb_culture_space_info = SeoulApiToCsvOperator(
        task_id = 'tb_culture_space_info',
        dataset_nm = 'culturalSpaceInfo',
        path = '/opt/airflow/files/culturalSpaceInfo/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name = 'culturalSpaceInfo.csv'
    )

    tb_culture_event_info >> tb_culture_space_info
    