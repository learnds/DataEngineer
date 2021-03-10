from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class IntegrityCheckOperator(BaseOperator):
    '''
    Dag Operator for running Integrity checker
    '''
    ui_color = '#89DA33'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check_query="",
                 table_name="",
                 *args, **kwargs):

        super(IntegrityCheckOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.check_query = check_query
        self.table_name = table_name

    def execute(self, context):
        self.log.info('Running Integrity checks')
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
                      
        data_quality_check_sql = self.check_query
        rec_count = redshift.get_records(data_quality_check_sql)
        num_records = rec_count[0][0]
        if num_records > 0:
            raise ValueError(f"Integrity check failed on {self.table_name}. There are {rec_count[0][0]} records violating integrity constraints in table")
            self.log.error(f"Data quality check failed on {self.table_name}. {rec_count[0][0]}  records violate integrity constraints")
            
        self.log.info("Integrity check passed")
        