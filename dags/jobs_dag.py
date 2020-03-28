import uuid
from datetime import datetime, timedelta
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
db_name = 'PostgreSQL'


def log(dag_id, db_name):
    return "{dag_id} start processing tables in database: {database}".format(dag_id=dag_id, database=db_name)


def push_run_id(**kwargs):
    return '{run_id} ended'.format(run_id=kwargs['run_id'])


def pull_user(**context):
    msg = context['ti'].xcom_pull(task_ids='execute_bash', key='return_value')
    return msg


def pull_the_result(**kwargs):
    ti = kwargs['ti']
    msg = ti.xcom_pull(task_ids='query_the_table', key='return_value')
    print("received message: '%s'" % msg)
    context = ti.get_template_context()
    print("context: {}".format(context))


def get_count_rows(**kwargs):
    hook = PostgresHook()
    query = hook.get_records(sql='SELECT COUNT(*) FROM {};'.format('table_name'))
    kwargs['ti'].xcom_push(key='{}_count'.format('table_name'), value=query[0][0])


def check_table_exist(sql_to_get_schema, sql_to_check_table_exist,
                      table_name):
    """ callable function to get schema name and after that check if table exist """
    hook = PostgresHook()
    # get schema name
    query = hook.get_records(sql=sql_to_get_schema)
    for result in query:
        if 'airflow' in result:
            schema = result[0]
            print(schema)
            break

    # check table exist
    query = hook.get_first(sql=sql_to_check_table_exist.format(schema, table_name))
    print(query)
    if query:
        return 'skip_table_creation'
    else:
        print("table {} does not exist".format(table_name))
        return 'create_table'


for config_key, config_value in config.items():

    def create_dag():

        with DAG(config_key,
                 default_args={'start_date': config_value['start_date']},
                 schedule_interval=config_value['schedule_interval']) as dag:

            _table_name = config_value['table_name']

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
                op_args=["SELECT * FROM pg_tables;",
                         "SELECT * FROM information_schema.tables "
                         "WHERE table_schema = '{}'"
                         "AND table_name = '{}';", _table_name])

            task_skip_table_creation = DummyOperator(
                task_id='skip_table_creation')

            task_create_table = PostgresOperator(
                task_id='create_table',
                sql='''CREATE TABLE {table_name}(
                custom_id integer NOT NULL, user_name VARCHAR (50) 
                NOT NULL, timestamp TIMESTAMP NOT NULL);'''.format(table_name=_table_name))

            task_insert_new_row = PostgresOperator(
                task_id='insert_new_row',
                trigger_rule=TriggerRule.ALL_DONE,
                sql='''INSERT INTO {table_name} VALUES
                (%s, '{{{{ ti.xcom_pull(task_ids='execute_bash', key='return_value') }}}}', %s);'''
                    .format(table_name=_table_name),
                parameters=(uuid.uuid4().int % 123456789, datetime.now()))

            task_query_the_table = PostgreSQLCountRowsOperator(
                task_id='query_the_table',
                table_name=_table_name)

            task_print_the_result = PythonOperator(
                task_id='print_the_result',
                python_callable=pull_the_result,
                provide_context=True)

            task_log_info >> task_get_username_bash >> task_check_table_exist >> \
                    [task_skip_table_creation, task_create_table] >> \
            task_insert_new_row >> task_query_the_table >> task_print_the_result

        return dag

    globals()[config_key] = create_dag()