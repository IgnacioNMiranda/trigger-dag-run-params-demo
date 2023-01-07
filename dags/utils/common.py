from ast import literal_eval
from airflow.operators.python import get_current_context


def get_context_params():
    context = get_current_context()
    params = context['params']
    if 'configuration' in params:
        params = {
            **params,
            **literal_eval(params['configuration'])
        }
        del params['configuration']
    return params
