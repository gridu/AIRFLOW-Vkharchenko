import uuid
from datetime import datetime
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.postgres_custom import PostgreSQLCountRowsOperator

config = {
    'dag_id_1': {'schedule_interval': None,
                 'start_date': datetime(2020, 4, 2),
                 'table_name': 'public.table_name_1',
                 'catchup': False},
    'dag_id_2': {'schedule_interval': '@daily',
                 'start_date': datetime(2020, 4, 2),
                 'table_name': 'table_name_2',
                 'catchup': False},
    'dag_id_3': {'schedule_interval': '@hourly',
                 'start_date': datetime(2020, 4, 2),
                 'table_name': 'public.table_name_3',
                 'catchup': False}
}

db_name = 'PostgreSQL'
default_postgres_schema = 'public'


def log(dag_id, db_name):
    return f'{dag_id} start processing tables in database: {db_name}'


def push_run_id(**kwargs):
    return f'{kwargs["run_id"]} ended'


def pull_user(**context):
    msg = context['ti'].xcom_pull(task_ids='execute_bash', key='return_value')
    return msg


def pull_the_result(**kwargs):
    ti = kwargs['ti']
    msg = ti.xcom_pull(task_ids='query_the_table', key='return_value')
    print(f'received message: {msg}')
    context = ti.get_template_context()
    print(f'context: {context}')


def get_count_rows(**kwargs):  # Code practice: install and use PostgeSQL (Part I)
    hook = PostgresHook()
    query = hook.get_records(sql=f'SELECT COUNT(*) FROM {_table_name};')
    kwargs['ti'].xcom_push(key=f'{_table_name}_count', value=query[0][0])


def check_table_exist(**kwargs):
    """ callable function to get schema name and after that check if table exist """
    table_name = kwargs.get('table_name')
    exists_task = kwargs.get('exists')
    not_exists_task = kwargs.get('not_exists')
    arr_table_name = table_name.split('.')
    if len(arr_table_name) is 2:
        schema, table = arr_table_name
    elif len(arr_table_name) is 1:
        table = arr_table_name[0]
        schema = default_postgres_schema
    else:
        print('Invalid argument "table_name"')
        return
    sql_to_check_table_exist = f"SELECT * FROM information_schema.tables " \
                               f"WHERE table_schema = '{schema}' AND table_name = '{table}' LIMIT 1;"

    hook = PostgresHook()
    # check table exist
    query_result = hook.get_first(sql=sql_to_check_table_exist.format(schema, table_name))
    if query_result:
        return exists_task
    else:
        print(f'table {table_name} does not exist')
        return not_exists_task


def create_dag(dag_id, default_args, schedule_interval, table_name, catchup):

    with DAG(dag_id,
             default_args=default_args,
             schedule_interval=schedule_interval,
             catchup=catchup) as dag:

        task_log_info = PythonOperator(
            task_id='log_info',
            provide_context=False,
            python_callable=log,
            op_kwargs={'dag_id': dag.dag_id, 'db_name': db_name})

        task_get_username_bash = BashOperator(
            task_id='execute_bash',
            bash_command='whoami',
            xcom_push=True)

        task_check_table_exist = BranchPythonOperator(
            task_id='check_table_exist',
            provide_context=True,
            python_callable=check_table_exist,
            op_kwargs={'table_name': table_name,
                       'not_exists': 'create_table',
                       'exists': 'skip_table_creation'})

        task_skip_table_creation = DummyOperator(
            task_id='skip_table_creation')

        task_create_table = PostgresOperator(
            task_id='create_table',
            sql=f'''CREATE TABLE {table_name}(
            custom_id integer NOT NULL, user_name VARCHAR (50) 
            NOT NULL, timestamp TIMESTAMP NOT NULL);''')

        task_insert_new_row = PostgresOperator(
            task_id='insert_new_row',
            trigger_rule=TriggerRule.ALL_DONE,
            sql=f'''INSERT INTO {table_name} VALUES (
            '{uuid.uuid4().int % 123456789}',
            '{{{{ ti.xcom_pull(task_ids='execute_bash', key='return_value') }}}}', 
            '{datetime.now()}' );''')  # VALUES (id, xcom_value, datetime)

        task_query_the_table = PostgreSQLCountRowsOperator(
            task_id='query_the_table',
            table_name=table_name)

        task_print_the_result = PythonOperator(
            task_id='print_the_result',
            python_callable=pull_the_result,
            provide_context=True)

        task_log_info >> task_get_username_bash >> task_check_table_exist >> \
        [task_skip_table_creation, task_create_table] >> \
        task_insert_new_row >> task_query_the_table >> task_print_the_result

    return dag


for _dag_id, dag_value in config.items():
    _default_args = {'start_date': dag_value['start_date']}
    _schedule_interval = dag_value['schedule_interval']
    _table_name = dag_value['table_name']
    _catchup = dag_value['catchup']
    globals()[_dag_id] = create_dag(dag_id=_dag_id, default_args=_default_args,
                                    schedule_interval=_schedule_interval,
                                    table_name=_table_name, catchup=_catchup)
