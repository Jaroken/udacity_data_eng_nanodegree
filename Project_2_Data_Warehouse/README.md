Data Warehouse Project
======================
By Kenneth Preston


How to run the python scripts
-----------------------------

After the dwh.cfg file is populated with proper credential information you then need to open a terminal window in the project directory and run the following two commands:
- python3 create_tables.py
- python3 etl.py

Explanation of the files in the repository
------------------------------------------
- create_tables.py:: This file creates the tables, and deletes existing tables if they already exist. This file should be run first to initialize the tables and prepare for the etl process
- etl.py:: This file loads the data from s3 into the staging tables and then into the analytic tables of the database
- dwh.cfg:: This file includes all the credentials needed to access and modify the database. These values can be attained within the aws console when the redshift cluster is launched.
- sql_queries.py:: This file includes all the drop, create table, and copy statements to create the tables and run the etl process. This file includes all the statements that define the database and data ingestion process.



Discussion
==========


In this readme file I will be providing my discussion of the data warehouse project for the startup Sparkify. It is my goal to address the following three subjects in this document:

    1.Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
    2.State and justify your database schema design and ETL pipeline.

1.Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
-------------------------------------------------------------------------------------------------------

Sparkify is a music streaming platform that want to move their customer and songs datasets into the cloud. They want to move their data assets into a format that better facilitates the analytics team to mine the data for insights. The analytics team is interested in understanding the songs users are listening to.

To accomplish this goal this project will design the ETL pipeline to extract data from S3, stages them in redshift, and ultimately stores the data in dimensional tables that are easy for the analytics team to query.

It is hoped that this analytic database can be used to enhance user's experience when streaming music and ultimately increase engagement and protect against user churn (i.e. users leaving the service).


2.State and justify your database schema design and ETL pipeline.
-------------------------------------------------------------------------------------------------------

The database schema used for this database was a basic star schema. This schema is optimized for queries on song plays. This design is justified because it is simple for the analytics team to query, understand, and ultimately derive insights on Sparkify's song plays dataset. The simplicity of the design also allows for fast queries as complex joins are not necessary. 

The center of the star schema is around sonplays as the fact table, while the dimension tables (as the points of the star) provide further details on the user, song, artist and time of the song plays.

For the pipeline used for populating the analytic database the following steps are implemented:
    1. load data from s3 into Redshift staging tables
    2. load data from staging tables into analytic tables

Using staging tables, as opposed to copying directly from s3 into the analytic database, is beneficial. Staging tables isolate the data sources from the analytics database. It is good practice to perform the data consolidation on the staging platform isolating this task from the production analytic database. This allows the analytic database to be performant for reads and writes at most times, while the staging database's resources are used for data consolidation. 

In terms of naming the columns and choosing the data types of the staging table, it was decided to keep the names identical to the source data names for simplicity and tracability back to the source data. Ultimately the analytic database column names are at times somewhat different, but these small discrepancies are minor and well documented in the etl process in the sql_queries.py file included in this project. 




