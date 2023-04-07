from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


"""Using this operator to load dimension table"""
"""We will use the truncate-insert style mentioned in the Project Instructions"""
"""Very similar in structure to fact, Truncate default is different"""
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = '',
                sql = '',
                table_name = '',
                truncate = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table_name = table_name
        self.truncate = truncate

    def execute(self, context):
        self.log.info("LoadDimensionOperator starting")
        postgres_hook = PostgresHook(redshift_conn_id = self.redshift_conn_id)
        """Truncating the table"""
        if self.truncate:
            self.log.info(f"Truncating the {self.table_name} table.")
            postgres_hook.run(f"TRUNCATE {self.table_name}")
        """Inserting into dimension table"""
        self.log.info(f"Insert into the {self.table_name} dimension table")
        postgres_hook.run(f"INSERT INTO {self.table_name} {self.sql}")
