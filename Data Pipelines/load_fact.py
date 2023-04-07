from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""Using this operator to load fact table"""
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = '',
                sql = '',
                table_name = '',
                truncate = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table_name = table_name
        self.truncate = truncate

    def execute(self, context):
        self.log.info("LoadFactOperator starting")
        postgres_hook = PostgresHook(redshift_conn_id = self.redshift_conn_id)
        """Truncate table option"""
        if self.truncate:
            self.log.info(f"Truncating the {self.table_name} table.")
            postgres_hook.run(f"TRUNCATE {self.table_name}")
        """Inserting into fact table"""
        self.log.info(f"Insert into the {self.table_name} fact table")
        postgres_hook.run(f"INSERT INTO {self.table_name} {self.sql}")