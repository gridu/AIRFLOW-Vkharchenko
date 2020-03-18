from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator

config = {
    'dag_id_1': {'schedule_interval': None,
                 'start_date': datetime(2018, 11, 11),
                 'table_name': 'table_name_1'},
    'dag_id_2': {'schedule_interval': timedelta(minutes=2),
                 'start_date': datetime(2018, 11, 11),
                 'table_name': 'table_name_2'},
    'dag_id_3': {'schedule_interval': timedelta(minutes=1),
                 'start_date': datetime(2018, 11, 11),
                 'table_name': 'table_name_3'}
}

dags = []
db_name = 'PostgreSQL'  # temporary, for testing purposes


def log(dag_id, db_name):
    return "{dag_id} start processing tables in database: {database}".format(dag_id=dag_id, database=db_name)


def check_table_exist(**kwargs):
    if True:
        return 'skip_table_creation'
    else:
        return 'create_table'


for config_key, config_value in config.items():
    def create_dag():
        with DAG(
                config_key,
                default_args={'start_date': config_value['start_date']},
                schedule_interval=config_value['schedule_interval']) as dag:

            task_log_info = PythonOperator(
                task_id='log_info',
                provide_context=False,
                python_callable=log,
                op_kwargs={'dag_id': dag.dag_id, 'db_name': db_name})

            task_echo_username_bash = BashOperator(
                task_id='execute_bash',
                bash_command='echo $USER')

            task_check_table_exist = BranchPythonOperator(
                task_id='check_table_exist',
                provide_context=True,
                python_callable=check_table_exist)

            task_skip_table_creation = DummyOperator(
                task_id='skip_table_creation')

            task_create_table = DummyOperator(
                task_id='create_table')

            task_insert_new_row = DummyOperator(
                task_id='insert_new_row',
                trigger_rule='none_failed')

            task_query_the_table = DummyOperator(
                task_id='query_the_table')

            task_log_info >> task_echo_username_bash >> task_check_table_exist >> \
            [task_skip_table_creation, task_create_table] >> task_insert_new_row >> task_query_the_table

        return dag


    globals()[config_key] = create_dag()
