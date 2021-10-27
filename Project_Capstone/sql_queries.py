import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

temp_dim_table_drop = "DROP TABLE IF EXISTS temp_dim;"
demo_dim_table_drop = "DROP TABLE IF EXISTS demo_dim;"
airport_dim_table_drop = "DROP TABLE IF EXISTS airport_dim"
immig_fact_table_drop = "DROP TABLE IF EXISTS immig_fact"
city_dim_table_drop = "DROP TABLE IF EXISTS city_dim"
port_dim_table_drop = "DROP TABLE IF EXISTS port_dim"
temp_dim_staging_drop =  "DROP TABLE IF EXISTS temp_dim_staging"
demo_dim_staging_drop =  "DROP TABLE IF EXISTS demo_dim_staging"
airport_dim_staging_drop =  "DROP TABLE IF EXISTS airport_dim_staging"
immig_fact_staging_drop =  "DROP TABLE IF EXISTS immig_fact_staging"
city_dim_staging_drop = "DROP TABLE IF EXISTS city_dim_staging"
port_dim_staging_drop = "DROP TABLE IF EXISTS port_dim_staging"

temp_dim_table_create = """
CREATE TABLE temp_dim (
    city_id int,
    month int4,
    avg_temp float,
    avg_temp_uncertainty float
    )
"""

demo_dim_table_create = """
CREATE TABLE demo_dim (
    city_id int,
    state varchar(256),
    median_age float,
    male_population int,
    female_population int,
    total_population int,
    number_of_veterans int,
    foreign_born int,
    average_household_size float,
    state_code varchar(50),
    race varchar(256)
    )
"""

airport_dim_table_create = """
CREATE TABLE airport_dim (
    type varchar(50),
    name varchar(256),
    elevation_ft int,
    state_code varchar(50),
    iso_country varchar(256),
    city_id int,
    port_id int,
    longitude float,
    latitude float
    )
"""
                         
immig_fact_table_create = """
CREATE TABLE immig_fact (
    id int IDENTITY(0,1),
    cicid int,
    "year" int,
    month int,
    city_id int,
    port_id int,
    addr varchar(256),
    arrival_date int,
    resp_age int,
    visa int,
    mode int,
    airline varchar(256),
    PRIMARY KEY (id)
    )
"""

city_dim_table_create = """
CREATE TABLE city_dim (
    city_id int IDENTITY(0,1),
    city_name varchar(256) UNIQUE,
    PRIMARY KEY (city_id)
    )
"""
                         
port_dim_table_create = """
CREATE TABLE port_dim (
    port_id int IDENTITY(0,1),
    port_name varchar(256) UNIQUE,
    PRIMARY KEY (port_id)
    )
"""

temp_dim_staging_create = """
CREATE TABLE temp_dim_staging (
    city varchar(256),
    month int,
    avg_temp float,
    avg_temp_uncertainty float
    )
"""

demo_dim_staging_create = """
CREATE TABLE demo_dim_staging (
    city varchar(256),
    state varchar(256),
    median_age float,
    male_population int,
    female_population int,
    total_population int,
    number_of_veterans int,
    foreign_born int,
    average_household_size float,
    state_code varchar(50),
    race varchar(256)
    )
"""

airport_dim_staging_table_create = """
CREATE TABLE airport_dim_staging (
    type varchar(50),
    name varchar(256),
    elevation_ft float,
    state_code varchar(50),
    iso_country varchar(256),
    city varchar(256),
    port varchar(256),
    longitude float,
    latitude float
    )
"""

immig_fact_staging_table_create = """
CREATE TABLE immig_fact_staging (
    cicid float,
    "year" float,
    month float,
    port varchar(256),
    addr varchar(256),
    arrival_date float,
    resp_age float,
    visa float,
    mode float,
    airline varchar(256)
    )
"""

city_dim_table_staging_create = """
CREATE TABLE city_dim_staging (city_name varchar(256))
"""
                         
port_dim_table_staging_create = """
CREATE TABLE port_dim_staging (port_name varchar(256))
"""

# STAGING TABLES

temp_dim_staging_copy = """
COPY temp_dim_staging (city, month, avg_temp, avg_temp_uncertainty)
FROM 's3://kenscapstoneproject/temp_dim.csv'
delimiter ';'
iam_role {}
""".format(config.get('IAM_ROLE', 'ARN'))
                         
demo_dim_staging_copy = """
COPY demo_dim_staging 
FROM 's3://kenscapstoneproject/demo_dim.csv'
delimiter ';'
iam_role {}
""".format(config.get('IAM_ROLE', 'ARN'))
                         
airport_dim_staging_copy = """
COPY airport_dim_staging 
FROM 's3://kenscapstoneproject/airport_dim.csv'
delimiter ';'
iam_role {}
""".format(config.get('IAM_ROLE', 'ARN'))
   
immig_fact_staging_copy = """
COPY immig_fact_staging 
FROM 's3://kenscapstoneproject/immig_fact'
FORMAT AS PARQUET
iam_role {}
""".format(config.get('IAM_ROLE', 'ARN'))

city_dim_staging_copy = """
COPY city_dim_staging 
FROM 's3://kenscapstoneproject/city_dim.csv'
delimiter ';'
iam_role {}

""".format(config.get('IAM_ROLE', 'ARN'))
   
port_dim_staging_copy = """
COPY port_dim_staging 
FROM 's3://kenscapstoneproject/port_dim.csv'
DELIMITER ';'
CSV
iam_role {}

""".format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

city_dim_insert = """
    insert into city_dim (city_name)
    select DISTINCT city_name
    from city_dim_staging
"""

port_dim_insert = """
    insert into port_dim (port_name) 
    select DISTINCT port_name
    from port_dim_staging 
"""

airport_dim_insert = """
    insert into airport_dim (type, name, elevation_ft, state_code,iso_country, city_id, port_id, longitude, latitude)
    select s.type, s.name, s.elevation_ft, s.state_code,  s.iso_country, c.city_id, p.port_id, s.longitude, s.latitude
    from airport_dim_staging s
    left join port_dim p on s.port = p.port_name
    left join city_dim c on s.city = c.city_name
"""

demo_dim_insert = """
    insert into demo_dim (city_id,state,median_age, male_population, female_population, total_population, number_of_veterans, foreign_born, average_household_size, state_code, race)
    select c.city_id, d.state, d.median_age, d.male_population, d.female_population, d.total_population, d.number_of_veterans,d.foreign_born,d.average_household_size,d.state_code, d.race
    from demo_dim_staging d
    left join city_dim c on d.city = c.city_name
"""

temp_dim_insert = """
    insert into temp_dim (city_id, month, avg_temp, avg_temp_uncertainty)
    select c.city_id, t.month, t.avg_temp, t.avg_temp_uncertainty
    from temp_dim_staging t
    left join city_dim c on t.city = c.city_name
"""

immig_fact_insert = """
    insert into immig_fact (cicid, "year", month, city_id, port_id, addr, arrival_date, resp_age, visa,  mode, airline)
    select i.cicid, i.year, i.month, a.city_id, p.port_id, i.addr, i.arrival_date,  i.resp_age, i.visa, i.mode, i.airline
    from immig_fact_staging i
    left join port_dim p on i.port = p.port_name
    left join airport_dim a on p.port_id = a.port_id
"""


create_table_queries = [temp_dim_table_create, demo_dim_table_create,airport_dim_table_create,immig_fact_table_create,city_dim_table_create,port_dim_table_create, temp_dim_staging_create, demo_dim_staging_create, airport_dim_staging_table_create, immig_fact_staging_table_create, city_dim_table_staging_create, port_dim_table_staging_create]

drop_table_queries = [temp_dim_table_drop, demo_dim_table_drop,airport_dim_table_drop,immig_fact_table_drop,city_dim_table_drop,port_dim_table_drop,temp_dim_staging_drop,
demo_dim_staging_drop,airport_dim_staging_drop,immig_fact_staging_drop, city_dim_staging_drop, port_dim_staging_drop]

copy_table_queries = [temp_dim_staging_copy, demo_dim_staging_copy, airport_dim_staging_copy, immig_fact_staging_copy, city_dim_staging_copy, port_dim_staging_copy]

insert_table_queries = [city_dim_insert, port_dim_insert, airport_dim_insert, demo_dim_insert, temp_dim_insert,immig_fact_insert]


