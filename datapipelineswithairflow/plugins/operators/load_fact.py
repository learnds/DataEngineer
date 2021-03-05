from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Loading fact table songplays')
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
        
#        self.log.info('Deleting staging table')
#        redshift.run("DELETE FROM {}".format(self.table))

       
        load_fact_sql = SqlQueries.songplay_table_insert
        redshift.run(load_fact_sql)
        self.log.info("Done loading fact table songplays")
                

        
                 
