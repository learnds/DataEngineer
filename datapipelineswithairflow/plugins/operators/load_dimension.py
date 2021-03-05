from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        self.log.info("Loading dimenstion table " + self.table )
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
                      
                
#        self.log.info('Deleting staging table')
#        redshift.run("DELETE FROM {}".format(self.table))
                      
        if self.table == "time":
            load_dim_sql = SqlQueries.time_table_insert
        elif self.table == "artist":
            load_dim_sql = SqlQueries.artist_table_insert
        elif self.table == "song":
            load_dim_sql = SqlQueries.song_table_insert
        elif self.table == "user":
            load_dim_sql = SqlQueries.user_table_insert
        
        self.log.info(load_dim_sql)
        redshift.run(load_dim_sql)
        self.log.info("Done loading dim table " + self.table)
