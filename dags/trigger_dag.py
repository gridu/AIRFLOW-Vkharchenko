from airflow.models import DAG
from datetime import datetime
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import Variable

dag_name = 'trigger_dag_test'
path_to_file = Variable.get('trigger_dag_path_to_file') or 'home/arkenstone/airflow/dags/test.txt'
path_to_dir = Variable.get('trigger_dag_path_to_dir') or 'home/arkenstone/airflow/trigger_dag_results'


def create_dag():
    with DAG(
            dag_name,
            default_args={'start_date': datetime(2020, 3, 18)},
            schedule_interval=None) as dag:
        task_check_file = FileSensor(
            task_id="check_file_exist",
            filepath=path_to_file,
            poke_interval=20,
            fs_conn_id="fs_default")

        task_trigger_dag = TriggerDagRunOperator(
            task_id="trigger_dag",
            trigger_dag_id="trigger_dag_test",  # Ensure this equals the dag_id of the DAG to trigger
        )

        task_create_subdag = SubDagOperator(
            task_id='process_results_subdag',
            subdag=create_subdag(parent_dag_name=dag_name,
                                 child_dag_name='process_results_subdag',
                                 args={'start_date': datetime(2020, 3, 18)}),
            default_args={'start_date': datetime(2020, 3, 18)})

        task_check_file >> task_trigger_dag >> task_create_subdag

        return dag


def create_subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None)
    with dag_subdag:

        task_check_if_dag_triggered = ExternalTaskSensor(
            task_id='check_triggered',
            external_task_id='trigger_dag',
            external_dag_id=dag_name)

        task_remove_file = BashOperator(
            task_id='remove_file',
            bash_command='cd / ; rm {{ params.path_to_file }}',
            params={'path_to_file': path_to_file})

        task_create_finished_timestamp_file = BashOperator(
            task_id='create_finished_timestamp_file',
            bash_command='cd / ; cd {{ params.path_to_dir }} ; touch finished_{{ ts_nodash }}',
            params={'path_to_dir': path_to_dir}
        )

        task_check_if_dag_triggered >> task_remove_file >> task_create_finished_timestamp_file

    return dag_subdag


globals()[dag_name] = create_dag()
