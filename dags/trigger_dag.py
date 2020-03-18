from airflow.models import DAG
from datetime import datetime
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator

dag_name = 'trigger_dag_test'


def create_dag():
    with DAG(
            dag_name,
            default_args={'start_date': datetime(2020, 3, 18)},
            schedule_interval=None) as dag:

        task_check_file = FileSensor(
            task_id="check_file_exist",
            filepath="home/arkenstone/airflow/dags/test.txt",
            poke_interval=20)

        task_trigger_dag = TriggerDagRunOperator(
            task_id="trigger_dag",
            trigger_dag_id="trigger_dag_test",  # Ensure this equals the dag_id of the DAG to trigger
            )

        task_remove_file = BashOperator(
            task_id='remove_file',
            bash_command='cd / ; rm home/arkenstone/airflow/dags/test.txt')

        task_check_file >> task_trigger_dag >> task_remove_file

        return dag


globals()[dag_name] = create_dag()
