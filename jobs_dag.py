from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

config = {
    'dag_id_1': {'schedule_interval': timedelta(minutes=3),
                 'start_date': datetime(2018, 11, 11),
                 'table_name': 'table_name_1'},
    'dag_id_2': {'schedule_interval': timedelta(minutes=5),
                 'start_date': datetime(2018, 11, 11),
                 'table_name': 'table_name_2'},
    'dag_id_3': {'schedule_interval': timedelta(minutes=7),
                 'start_date': datetime(2018, 11, 11),
                 'table_name': 'table_name_3'}
}

dags = []
db_name = 'PostgreSQL'  # temporary, for testing purposes


def log(dag_id, db_name):
    return "{dag_id} start processing tables in database: {database}" \
        .format(dag_id=dag_id, database=db_name)


for config_key, config_value in config.items():
    with DAG(
            config_key,
            default_args={'start_date': config_value['start_date']},
            schedule_interval=config_value['schedule_interval']) as dag:
        t1 = PythonOperator(
            task_id='log_info',
            provide_context=False,
            python_callable=log,
            op_kwargs={'dag_id': dag.dag_id, 'db_name': db_name})
        t2 = DummyOperator(
            task_id='insert_new_row')
        t3 = DummyOperator(
            task_id='query_the_table')
