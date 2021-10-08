from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 redshift_conn_id = "",
                 sql_test = "",
                 sql_result = 0,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id,
        self.sql_test = sql_test,
        self.sql_result = sql_result
        

    def execute(self, context):
        self.log.info('DataQualityOperator: Checking the data itself for nulls')
        

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift_hook.get_records(self.sql_test)
        
        # check if test failed
        if records[0] != self.sql_result:
            raise ValueError("""
                Too many Nulls found. \
                expected {}, but {} found 
            """.format(self.sql_result, records[0]))
        else:
            self.log.info("Data quality check passed")
        
        
        