import psycopg2
import configparser
import pandas as pd

def check_count_port(port_name, immig_fact):
    """
    Checks port counts for specific port between database and source file
    parameters:
    port_name (str): name of the port
    immig_fact (pyspark dataframe): the immigration dataframe
    """
    port_count_source =immig_fact.select('port').groupby('port').count().toPandas()
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    query = 'select port_name, count(immig_fact.port_id) from immig_fact join port_dim on port_dim.port_id = immig_fact.port_id group by port_name'
    port_count_db = pd.read_sql_query(query, conn)
    
    result = (port_count_db[port_count_db.port_name == port_name].reset_index(drop=True)['count'][0] == port_count_source[port_count_source.port ==port_name].reset_index(drop=True)['count'][0])
    
    return('Port count matched between database and source for port {} : {}'.format(port_name,result))

def check_no_nulls(table_name, column_name):
    """
    Checks if column in table has no nulls.
    parameters:
    table_name (str): name of the table 
    column_name (str): name of the table column to test
    """
  
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    query = 'select count({}) as county from {} where {} is NULL'.format(column_name, table_name, column_name)
    count_db_null = pd.read_sql_query(query, conn)
    count_db_null.iloc[0,0]
       
    return('Null counts is zero for {} in {} : {}'.format(column_name, table_name, count_db_null.iloc[0,0] == 0))