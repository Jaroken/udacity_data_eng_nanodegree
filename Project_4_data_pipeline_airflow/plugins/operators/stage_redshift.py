from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 table='',
                 redshift_conn_id='',
                 aws_credentials_id='',
                 s3_bucket = '',
                 s3_key='',
                 region='',
                 copy_json_option='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table=table,        
        self.redshift_conn_id=redshift_conn_id,
        self.aws_credentials_id=aws_credentials_id,
        self.s3_bucket = s3_bucket,
        self.s3_key=s3_key,
        self.region=region,
        self.copy_json_option=copy_json_option

    def execute(self, context):
        self.log.info('StageToRedshiftOperator: AWS Hook, credentials, and redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        aws_hook = AwsHook(self.aws_credentials_id)
        creds = aws_hook.get_credentials()
        
        self.log.info('StageToRedshiftOperator: Copying S3 data')
        
        s3_path = "s3://"+str(self.s3_bucket)+"/"+str(self.s3_key.format(**context))
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            creds.access_key,
            creds.secret_key,
            self.region,
            self.copy_json_option
        )
        redshift.run(formatted_sql)



