from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table="",
                 redshift_conn_id="",
                 query_sql="",
                 append_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query_sql = query_sql
        self.append_table = append_table
    def execute(self, context):
        self.log.info('LoadFactOperator: loading dimension data for {}'.format(self.table))

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_table == False:
            redshift_hook.run("DELETE FROM {}".format(self.table))
            
        redshift_hook.run('INSERT INTO {} {}'.format(self.table, self.query_sql))
        
