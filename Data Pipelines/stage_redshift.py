from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    sql = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{{}}'
        SECRET_ACCESS_KEY '{{}}'
        {} REGION '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 aws_cred_id = '',
                 table_name = '',
                 s3_path = '',
                 region = '',
                 truncate = False,
                 format = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_cred_id = aws_cred_id
        self.table_name = table_name
        self.s3_path = s3_path
        self.region = region
        self.truncate = truncate
        self.format = format

    def execute(self, context):
        self.log.info('StageToRedshiftOperator begin process')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_cred_id)
        creds = aws_hook.get_credentials()

        """Again, truncating the redshift table"""
        if self.truncate:
            self.log.info(f"Truncating {self.table_name}")
            redshift.run(f"TRUNCATE {self.table_name}")   

        self.log.info("Copy data from s3 over to Redshift.")    
        s3_path = f"s3://{self.s3_path}"

        run_sql = StageToRedshiftOperator.sql.format(
            self.table_name,
            s3_path,
            creds.access_key,
            creds.secret_key,
            self.format,
            self.region
        )
        redshift.run(run_sql)




