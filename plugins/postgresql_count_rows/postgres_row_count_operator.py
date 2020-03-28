import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)


class PostgreSQLCountRowsOperator(BaseOperator):
    """ operator to count rows in the table"""

    @apply_defaults
    def __init__(self, table_name,
                 *args, **kwargs):
        self.table_name = table_name
        self.hook = PostgresHook()
        super(PostgreSQLCountRowsOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        query = 'SELECT COUNT(*) FROM {};'.format(self.table_name)
        result = self.hook.get_first(sql=query)
        log.info('Row count of {table_name}: {count}'.format(table_name=self.table_name, count=result[0]))
        return result[0]


class PostgresCustomOperatorsPlugin(AirflowPlugin):
    name = "postgres_custom"
    operators = [PostgreSQLCountRowsOperator]
