from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table_list = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list
"""Here we will create our operator used to perform quality checks on tables."""
def execute(self, context):
        self.log.info('Starting process')
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        """The test will check to see if we actually loaded data"""
        # This code is resued from the Data Quality lesson: 16. Solution 4: Data Quality
        for table in self.table_list:
            values = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(values) < 1 or len(values[0]) < 1:
                raise ValueError(f"Quality Check failed. {table} has no results.")
            num_records = values[0][0]
            if num_records < 1:
                raise ValueError(f"Quality Check failed. {table} has 0 rows.")
            self.log.info(f"Data Quality on {table} check passed with {values[0][0]} records.")
