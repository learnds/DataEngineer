from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    '''
    Dag operator for loading staging tables into Redshift derived from BasedOperator
    '''
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            FORMAT AS {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id = "",
                 table="",
                 s3_bucket = "",
                 s3_key = "",
                 fileformat = "",
                 truncate_flag="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.fileformat = fileformat
        self.truncate_flag = truncate_flag
        
        

    def execute(self, context):

        self.log.info('Loading staging table')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
        
        if self.truncate_flag == "Y":
           self.log.info("Truncating table {}".format(self.table))
           redshift.run("truncate table {}".format(self.table))
        
        self.log.info("Copying from S3 to redshift")
        rend_key = self.s3_key.format(**context)
        s3_path = "s3://{}{}".format(self.s3_bucket,rend_key)
       
        final_copy_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.fileformat
        )
        redshift.run(final_copy_sql)
        self.log.info(f"Staging table {self.table} loaded successfully")

        
       
        
        
        
        
        
        
        





