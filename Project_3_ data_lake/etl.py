import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('Credentials','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('Credentials','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Creates spark session 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Ingesting song data, extracting columns, and writing parquet output files.
    
    Params
    
    spark: spark session to use
    input_data: s3 location of song data
    output_data: location where to write output parquet files
    
    """
    
    
    # get filepath to song data file
    # Example location song_data/A/B/C/TRABCEI128F424C983.json
    song_data = '{}/song_data/*/*/*/*.json'.format(input_data)
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist").mode("overwrite").parquet("{}/song_table/song_table.parquet".format(output_data))

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").distinct()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet("{}/artist_table/artist_table.parquet".format(output_data))


def process_log_data(spark, input_data, output_data):
    """
    Ingesting log data, extracting columns, and writing parquet output files.
    
    Params
    
    spark: spark session to use
    input_data: s3 location of log data
    output_data: location where to write output parquet files
    
    """
    
    # get filepath to log data file
    log_data = '{}/log_data/*/*/*/*.json'.format(input_data)

    # read log data file
    df = spark.read.json(song_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").distinct()
    
    # write users table to parquet files
    users_table = users_table.write.mode("overwrite").parquet("{}/users_table/users_table.parquet".format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(round(int(x)/1000, 0))))
    df = df.withColumn('time_stamp', get_timestamp(df.select('ts')))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(round(int(x)/1000, 0)))))
    df = df.withColumn('date_time', get_timestamp(df.select('ts')))
    
    # extract columns to create time table
    # start_time, hour, day, week, month, year, weekday
    time_table = df.select(col('date_time').alias('start_time'),
                           hour('date_time').alias('hour'),
                           dayofmonth('date_time').alias('day'),
                           weekofyear('date_time').alias('week'),
                           month('date_time').alias('month'),
                           year('date_time').alias('year'),
                           dayofweek('date_time').alias('weekday'))
    
    
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet("{}/time_table/time_table.parquet".format(output_data))

    # read in song data to use for songplays table
    song_df = spark.read.json('{}/song_data/*/*/*/*.json'.format(input_data))

    # extract columns from joined song and log datasets to create songplays table 
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = df.join(other = song_df, on = song_df.artists_name == df.artist, how = "inner").select(
    col("df.datetime").alias("start_time"),
    col("df.userId").alias("user_id"),
    col("df.level").alias("level"),
    col("song_df.song_id").alias("song_id"),
    col("song_df.artist_id").alias("artist_id"),
    col('df.sessionId').alias('session_id'),
    col('df.location').alias('location'),
    col('df.userAgent').alias('user_agent'),
    year('df.datetime').alias('year'),
    month('df.datetime').alias('month')).withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.partitionBy("year", "month").mode("overwrite").parquet("{}/songplays_table/songplays_table.parquet".format(output_data))


def main():
    """
    Runs the main program to load in the song and log data, process it, then send it to an s3 bucket.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://kp-udacity"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
