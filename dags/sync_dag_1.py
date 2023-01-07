# Sync DAG 1
from airflow.decorators import task, dag
from datetime import datetime
import logging

from dags.utils.common import get_context_params


@dag(
    start_date=datetime(2023, 1, 7),
    default_view='graph',
    schedule_interval='@daily',
    catchup=False,
)
def sync_dag_1():
    @task.python
    def sync():
        logging.info('Syncing data 1...')
        # Access to context params in order to perform certain tasks
        params = get_context_params()
        logging.debug(f'params: {params}')
        if 'run-task-a' in params and params['run-task-a']:
            logging.info('Running task A...')
        elif 'run-task-b' in params and params['run-task-b']:
            logging.info('Running task B...')
    sync()


sync_dag_1()
