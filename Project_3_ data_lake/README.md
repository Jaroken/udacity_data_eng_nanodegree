# Data Lake Project
This is writeup for the data lake project for the udacity nano degree for data engineering program. 

## Introduction
The ficticious music streaming startup, Sparkify, want to move their data warehouse to a data lake. Their data is in S3, in a directory of JSON logs and JSON metadata of the songs data.

This project builds an ETL pipeline that moves sparkify data from S3 to process it in Spark and then loads the data as dimensional tables into S3. 

Sparify's analytic goal is to create dimensional tables that allow their analytics team to continue finding insights in what songs their users are listening to.

## How to run 
1. create s3 bucket and assign that location to the main function's  output_data variable
2. in terminal run: python etl.py

## Database schema

The database schema used for this database was a basic star schema. This schema is optimized for queries on song plays. This design is justified because it is simple for the analytics team to query, understand, and ultimately derive insights on Sparkify's song plays dataset. The simplicity of the design also allows for fast queries as complex joins are not necessary. 

The center of the star schema is around sonplays as the fact table, while the dimension tables (as the points of the star) provide further details on the user, song, artist and time of the song plays.

Here are the tables that were created:
### Fact Table
- songplays - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### Dimension Tables
- users - users in the app
- user_id, first_name, last_name, gender, level
- songs - songs in music database
- song_id, title, artist_id, year, duration
- artists - artists in music database
- artist_id, name, location, lattitude, longitude
- time - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday

## ETL Pipeline
For the pipeline used for populating the analytic database the following steps are implemented:
    1. load data from s3
    2. transform the data using spark
    3. load transformed data as dimensional analytic tables to a new s3 location
    
Spark was used because it effectively processes large datasets from s3 back to a new s3 location in memory. 
 
