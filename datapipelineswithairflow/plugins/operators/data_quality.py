from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check_query="",
                 expected_count="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.check_query = check_query
        self.expected_count = expected_count

    def execute(self, context):
        self.log.info('Running Data quality checks')
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
                      
        data_quality_check_sql = self.check_query
        rec_count = redshift.get_records(data_quality_check_sql)
        num_records = rec_count[0][0]
        if num_records != self.expected_count:
            raise ValueError(f"Data quality check failed. {rec_count[0][0]} does not match expected count {self.expected_count}")
            self.log.error(f"Data quality check failed. {rec_count[0][0]} does not match expected count {self.expected_count}")
        
        self.log.info("Data quality check passed")
        