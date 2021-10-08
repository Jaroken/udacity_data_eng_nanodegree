from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table="",
                 redshift_conn_id="",
                 query_sql="",
                 truncate="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query_sql = query_sql,
        self.truncate = truncate

    def execute(self, context):
        self.log.info('LoadDimensionOperator: loading dimension data for {}'.format(self.table))

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            redshift_hook.run('TRUNCATE TABLE {}'.format(self.table))
            
            
        redshift_hook.run('INSERT INTO {} {}'.format(self.table, self.query_sql))
        