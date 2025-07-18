import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


with DAG(
    dag_id='dags_trigger_dag_run_operator',
    start_date=pendulum.datetime(2025,7,1, tz='Asia/Seoul'),
    schedule='30 9 * * *',
    catchup=False
) as dag:
    start_task = BashOperator(
        task_id = 'start_task',
        bash_command = 'echo "start!"'
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id = 'trigger_dag_task',
        trigger_dag_id = 'dags_python_operator', #trigger해서 실행할 dag id
        trigger_run_id = None, #run_id : dag의 수행 방시과 시간을 유일하게 식별해주는 키, 수행 방식(schedule/backfill/manual)에 따라 키가 달라짐
        # execution_date = '{{data_interval_end}}', #시간값을 주게 되면 manual 실행으로 간주
        logical_date='{{data_interval_start}}', #3.0버전 부터는 'logical_date'가 공식이고, execution_date는 미사용 및 소멸 
        reset_dag_run = True, #True이면 이미 run_id로 수행된 이력이 있어도 재수행하게 됨(부모 트리거가 backfill 될 때 리셋하고 재실행 됨).
        wait_for_completion = False, #트리거 dag 완료를 대기할지 여부
        poke_interval = 60, #트리거 dag 완료 확인 주기(초), 기본 60초
        allowed_states = ['success'], #트리거 시키는 dag(현재 dag)이 success로 표시하고자 하는 트리거된 dag의 결과값을 list로 대입
        failed_states = None #트리거 시키는 dag(현재 dag)이 fail로 표시하고자 하는 트리거된 dag의 결과값을 list로 대입
    )

    start_task >> trigger_dag_task

