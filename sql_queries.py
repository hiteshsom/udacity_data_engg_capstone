import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_bay_area_trips_drop = "DROP TABLE IF EXISTS STAGING_BAY_AREA_TRIPS"
staging_nyc_trips_drop = "DROP TABLE IF EXISTS STAGING_NYC_TRIPS"
trips_table_drop = "DROP TABLE IF EXISTS TRIPS"
stations_table_drop = "DROP TABLE IF EXISTS STATIONS"
bikes_table_drop = "DROP TABLE IF EXISTS BIKES"
time_table_drop = "DROP TABLE IF EXISTS TIME"

# CREATE TABLES

staging_bay_area_trips_table_create = ("""CREATE TABLE STAGING_BAY_AREA_TRIPS
(
duration_sec int,
start_time timestamp,
end_time timestamp,
start_station_id varchar(5000),
start_station_name varchar(5000),
start_station_latitude float,
start_station_longitude float,
end_station_id varchar(5000),
end_station_name varchar(5000),
end_station_latitude float,
end_station_longitude float,
bike_id varchar(5000),
user_type varchar(5000),
member_birth_year int,
member_gender varchar(5000),
bike_share_all varchar(5000)
)
""")

staging_nyc_trips_table_create = ("""CREATE TABLE STAGING_NYC_TRIPS
(
duration_sec int,
start_time timestamp,
end_time timestamp,
start_station_id varchar(5000),
start_station_name varchar(5000),
start_station_latitude float,
start_station_longitude float,
end_station_id varchar(5000),
end_station_name varchar(5000),
end_station_latitude float,
end_station_longitude float,
bike_id varchar(5000),
user_type varchar(5000),
member_birth_year int,
member_gender int
)
""")


trips_table_create = ("""CREATE TABLE TRIPS
(
TRIP_ID INT IDENTITY(1,1),
START_TIME TIMESTAMP,
END_TIME TIMESTAMP,
START_STATION_ID VARCHAR(5000),
END_STATION_ID VARCHAR(5000),
BIKE_ID VARCHAR(5000),
DURATION INT,
USER_TYPE VARCHAR(5000),
MEMBER_BIRTH_YEAR INT,
MEMBER_GENDER VARCHAR(5000)
)
""")


stations_table_create = ("""CREATE TABLE STATIONS
(
STATION_ID VARCHAR(5000),
STATION_NAME VARCHAR(5000),
STATION_LATITUDE FLOAT,
STATION_LONGITUDE FLOAT
)
""")


bikes_table_create = ("""CREATE TABLE BIKES
(
BIKE_ID Varchar(5000)
)
""")


time_table_create = ("""CREATE TABLE TIME
(
TS TIMESTAMP,
HOUR INT,
DAY INT,
WEEK INT,
MONTH INT,
YEAR INT,
WEEKDAY VARCHAR(5000)
)
""")

# STAGING TABLES

staging_bay_area_trips_copy = ("""
    copy staging_bay_area_trips from {} 
    format as CSV 
    ignoreheader 1
    delimiter as ','
    credentials 'aws_iam_role={}' 
    region 'us-west-2' 
    timeformat 'auto' 
""").format(config.get('S3', 'BAY_AREA_DATA'), config.get('IAM_ROLE', 'ARN'))

staging_nyc_trips_copy = ("""
    copy staging_nyc_trips from {} 
    format as CSV 
    ignoreheader 1
    delimiter as ','
    credentials 'aws_iam_role={}' 
    region 'us-west-2' 
    timeformat 'auto' 
""").format(config.get('S3', 'NYC_DATA'), config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

trips_table_insert_nyc = ("""Insert into 
trips(start_time, end_time, start_station_id, end_station_id, bike_id, duration, user_type, member_birth_year, 
member_gender)
Select 
start_time, 
end_time, 
concat('nyc_', start_station_id) as start_station_id, 
concat('nyc_', end_station_id) as end_station_id,  
concat('nyc_', bike_id) as bike_id, 
duration_sec, 
user_type, 
member_birth_year, 
case member_gender
when 0 then 'Unknown/Other'
when 1 then 'Male'
else 'Female'
end 
from staging_nyc_trips
where start_time is not null and end_time is not null and start_station_id is not null and end_station_id is not null
and bike_id is not null and duration_sec is not null and user_type is not null and member_birth_year is not null and 
member_gender is not null
""")

trips_table_insert_bay_area = ("""Insert into
trips(start_time, end_time, start_station_id, end_station_id, bike_id, duration, user_type, member_birth_year,
member_gender)
Select
start_time,
end_time,
concat('bay_area_', start_station_id) as start_station_id,
concat('bay_area_', end_station_id) as end_station_id,
concat('bay_area_', bike_id) as bike_id,
duration_sec,
user_type,
member_birth_year,
case member_gender
when 'Other' then 'Unknown/Other'
when 'Male' then 'Male'
else 'Female'
end
from staging_bay_area_trips
where start_time is not null and end_time is not null and start_station_id is not null and end_station_id is not null
and bike_id is not null and duration_sec is not null and user_type is not null and member_birth_year is not null and
member_gender is not null
""")


stations_table_insert_nyc_start = ("""
Insert into stations(station_id, station_name, station_latitude, station_longitude)
Select start_station_id, start_station_name, start_station_latitude, start_station_longitude from 
(
Select *, row_number() over (partition by start_station_id) as row_num from staging_nyc_trips
)
where row_num = 1 and start_station_id is not null and start_station_name is not null and start_station_latitude is not 
null and start_station_longitude is not null
""")


stations_table_insert_nyc_end = ("""
Insert into stations(station_id, station_name, station_latitude, station_longitude)
Select end_station_id, end_station_name, end_station_latitude, end_station_longitude from
(
Select *, row_number() over (partition by end_station_id) as row_num from staging_nyc_trips
)
where row_num = 1 and end_station_id is not null and end_station_name is not null and end_station_latitude is not null
and end_station_longitude is not null
""")

stations_table_insert_bay_area_start = ("""
Insert into stations(station_id, station_name, station_latitude, station_longitude)
Select start_station_id, start_station_name, start_station_latitude, start_station_longitude from
(
Select *, row_number() over (partition by start_station_id) as row_num from staging_bay_area_trips
)
where row_num = 1 and start_station_id is not null and start_station_name is not null and start_station_latitude is not 
null and start_station_longitude is not null 
""")

stations_table_insert_bay_area_end = ("""
Insert into stations(station_id, station_name, station_latitude, station_longitude)
Select end_station_id, end_station_name, end_station_latitude, end_station_longitude from
(
Select *, row_number() over (partition by end_station_id) as row_num from staging_bay_area_trips
)
where row_num = 1 and end_station_id is not null and end_station_name is not null and end_station_latitude is not null
and end_station_longitude is not null 
""")

bikes_table_insert_nyc = ("""
Insert into bikes(bike_id)
Select bike_id
from 
(
Select *, row_number() over (partition by bike_id) as row_num from staging_nyc_trips
)
where row_num = 1 and bike_id is not null
""")

bikes_table_insert_bay_area = ("""
Insert into bikes(bike_id)
Select bike_id
from 
(
Select *, row_number() over (partition by bike_id) as row_num from staging_nyc_trips
)
where row_num = 1 and bike_id is not null
""")

time_table_insert_nyc_start = ("""
Insert into time 
SELECT TS, DATEPART(h, TS) AS HOUR, DATEPART(d, TS) AS DAY,
DATEPART(w, TS) AS WEEK, DATEPART(mon, TS) AS MONTH, DATEPART(y, TS) AS YEAR, 
DATEPART(dow, TS) AS WEEKDAY FROM 
(SELECT DISTINCT start_time as TS FROM 
(
Select *, row_number() over (partition by start_time) as row_num from staging_nyc_trips
)
where row_num = 1 and TS is not null
)
""")

time_table_insert_nyc_end = ("""
Insert into time 
SELECT TS, DATEPART(h, TS) AS HOUR, DATEPART(d, TS) AS DAY,
DATEPART(w, TS) AS WEEK, DATEPART(mon, TS) AS MONTH, DATEPART(y, TS) AS YEAR, 
DATEPART(dow, TS) AS WEEKDAY FROM 
(SELECT DISTINCT end_time as TS FROM 
(
Select *, row_number() over (partition by end_time) as row_num from staging_nyc_trips
)
where row_num = 1 and TS is not null
)
""")


time_table_insert_bay_area_start = ("""
Insert into time 
SELECT TS, DATEPART(h, TS) AS HOUR, DATEPART(d, TS) AS DAY,
DATEPART(w, TS) AS WEEK, DATEPART(mon, TS) AS MONTH, DATEPART(y, TS) AS YEAR, 
DATEPART(dow, TS) AS WEEKDAY FROM 
(SELECT DISTINCT start_time as TS FROM
(
Select *, row_number() over (partition by start_time) as row_num from staging_bay_area_trips
)
where row_num = 1 and TS is not null)
""")

time_table_insert_bay_area_end = ("""
Insert into time 
SELECT TS, DATEPART(h, TS) AS HOUR, DATEPART(d, TS) AS DAY,
DATEPART(w, TS) AS WEEK, DATEPART(mon, TS) AS MONTH, DATEPART(y, TS) AS YEAR, 
DATEPART(dow, TS) AS WEEKDAY FROM 
(SELECT DISTINCT end_time as TS FROM
(
Select *, row_number() over (partition by end_time) as row_num from staging_bay_area_trips
)
where row_num = 1 and TS is not null
)
""")

# Data Quality Check
trips_check = """Select count(*) from trips"""
stations_check = """Select count(*) from stations"""
bikes_check = """Select count(*) from bikes"""
time_check = """Select count(*) from time"""
trips_missing_check = """Select count(*) from trips where start_time is null or end_time is null or 
start_station_id is null or end_station_id is null or bike_id is null or duration is null or user_type is null or 
member_birth_year is null or member_gender is null"""
stations_missing_check = """Select count(*) from stations where station_id is null or station_name is null
or station_latitude is null or station_longitude is null"""
bikes_missing_check = """Select count(*) from bikes where bike_id is null"""
time_missing_check = """Select count(*) from time where TS is null or HOUR  is null or DAY  is null or WEEK  is null or 
MONTH  is null or YEAR  is null or WEEKDAY is null"""

# QUERY LISTS

create_table_queries = [staging_nyc_trips_table_create, staging_bay_area_trips_table_create, trips_table_create,
                        stations_table_create, bikes_table_create, time_table_create]
drop_table_queries = [staging_nyc_trips_drop, staging_bay_area_trips_drop, trips_table_drop, stations_table_drop,
                      bikes_table_drop, time_table_drop]
copy_table_queries = [staging_bay_area_trips_copy, staging_nyc_trips_copy]
insert_table_queries = [trips_table_insert_nyc, trips_table_insert_bay_area,
                        stations_table_insert_nyc_start, stations_table_insert_nyc_end,
                        stations_table_insert_bay_area_start, stations_table_insert_bay_area_end,
                        bikes_table_insert_nyc, bikes_table_insert_bay_area,
                        time_table_insert_nyc_start, time_table_insert_nyc_end,
                        time_table_insert_bay_area_start, time_table_insert_bay_area_end]
data_quality_check_records = [trips_check, stations_check, bikes_check, time_check]
data_quality_check_missing = [trips_missing_check, stations_missing_check, bikes_missing_check, time_missing_check]
