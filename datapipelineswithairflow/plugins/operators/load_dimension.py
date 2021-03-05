from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table_query = table_query

    def execute(self, context):
        self.log.info("Loading dimenstion table -" + self.table_query)
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
                      
        load_dim_sql = self.table_query
        redshift.run(load_dim_sql)
        self.log.info("Done loading dim table ")
