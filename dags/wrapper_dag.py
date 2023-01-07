# Wrapper DAG
import logging
from datetime import datetime

from airflow.decorators import task, dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule


@dag(
    start_date=datetime(2023, 1, 7),
    default_view='graph',
    schedule_interval='@daily',
    catchup=False,
)
def wrapper_dag():
    @task.python
    def create_backup_env():
        print('Creating backup env...')

    trigger_sync_dag_1_task_id = 'trigger_sync_dag_1'
    trigger_sync_dag_1_task = TriggerDagRunOperator(
        task_id=trigger_sync_dag_1_task_id,
        trigger_dag_id='sync_dag_1',
        conf={'configuration': '{{ params }}'},
        wait_for_completion=True,
        poke_interval=60,
        failed_states=[State.FAILED],
    )

    skip_sync_1_task_id = 'skip_sync_1'
    skip_sync_1_task = PythonOperator(
        task_id=skip_sync_1_task_id,
        python_callable=lambda: logging.info("""
        Sync 1 was cancelled.
        In order to run it, include ("run-sync-1": true) in the dag run config.
        """),
    )

    def trigger_sync_1(**context):
        param_key = 'run-sync-1'
        params = context['params']
        if (param_key in params and not params[param_key]):
            return skip_sync_1_task_id
        return trigger_sync_dag_1_task_id

    sync_1_branching_task = BranchPythonOperator(
        task_id='sync_1_branching',
        python_callable=trigger_sync_1,
    )

    trigger_sync_dag_2_task = TriggerDagRunOperator(
        task_id='trigger_sync_dag_2',
        trigger_dag_id='sync_dag_2',
        conf={'configuration': '{{ params }}'},
        wait_for_completion=True,
        poke_interval=60,
        failed_states=[State.FAILED],
    )

    skip_sync_2_task_id = 'skip_sync_2'
    skip_sync_2_task = PythonOperator(
        task_id=skip_sync_2_task_id,
        python_callable=lambda: logging.info("""
        Sync 2 was cancelled.
        In order to run it, include ("run-sync-2": true) in the dag run config.
        """),
    )

    def trigger_sync_2(**context):
        param_key = 'run-sync-2'
        params = context['params']
        if (param_key in params and not params[param_key]):
            return skip_sync_1_task_id
        return trigger_sync_dag_1_task_id

    sync_2_branching_task = BranchPythonOperator(
        task_id='sync_2_branching',
        python_callable=trigger_sync_2,
    )

    join_branching_task = EmptyOperator(
        task_id='join_branching',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    @task.python
    def send_audit_email():
        logging.info('Sending Audit email...')

    create_backup_env() >> [sync_1_branching_task, sync_2_branching_task]

    sync_1_branching_task >> trigger_sync_dag_1_task
    sync_1_branching_task >> skip_sync_1_task

    sync_2_branching_task >> trigger_sync_dag_2_task
    sync_2_branching_task >> skip_sync_2_task

    [trigger_sync_dag_1_task, skip_sync_1_task, trigger_sync_dag_2_task,
        skip_sync_2_task] >> join_branching_task
    join_branching_task >> send_audit_email()


wrapper_dag()
