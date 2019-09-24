# Bike Sharing Data Model
## Project Summary
This project aims at developing a data model of bike sharing system for easy analysis.

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

### Step 1: Scope the Project and Gather Data

#### Scope
Explain what you plan to do in the project in more detail. What data do you use? What is your end solution look like? What tools did you use? etc>

The data is about bike sharing rides of two cities New York city and Bay Area.
The data exist for August, 2019 of both the cities.
The end use case for this is to make data available in a format suitable for analysis.
The tools I am going to use is jupyter lab.

#### Describe and Gather Data
Describe the data sets you're using. Where did it come from? What type of information is included?
The dataset are about bike rides in New york city and Bay Area.

The data set were downloaded from
1. https://www.citibikenyc.com/system-data
2. https://s3.amazonaws.com/baywheels-data/index.html

The information included is about bike ride and some information about rider.
For example: duration of ride, bike start station location, bike end_station_location, gender of the rider, birth_year etc.

### Step 2: Explore and Assess the Data
#### Explore the Data
Identify data quality issues, like missing values, duplicate data, etc.
- We have few missing in both the tables which we have addressed in query of inserting data in fact and dimension tables
- The column names are not same which is handled by the insertion query of fact and dimension table
- There are no duplication in data but when we will make the star schema we might get some duplications in dimension
tables which we have addressed in the query of inserting data in fact and dimension tables

#### Cleaning Steps
Document steps necessary to clean the data
- Remove the missing values
- The keep column names same
- Remove duplications in dimension tables


### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
Map out the conceptual data model and explain why you chose that model
The conceptual data model will be star schema.
Dimension tables: stations, bikes and time
Fact: trips

#### 3.2 Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model
1. Make dimensional tables
2. Make fact tables

### Step 4: Run Pipelines to Model the Data
#### 4.1 Create the data model
Build the data pipelines to create the data model.
The data pipeline is built in etl.py


#### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
Data quality check involves a count of number of records in fact and dimension tables should be more than 0.0

#### 4.3 Data dictionary
Create a data dictionary for your data model. For each field, provide a brief description of what the data is and
where it came from. You can include the data dictionary in the notebook or in a separate file.

Tables
TRIPS
trip_id: Id column of the fact table made using Identity(1,1) function. Datatype is Integer.
start_time: Starting timestamp.
end_time: Ending timestamp.
start_station_id: Id of the starting station of the Bike ride. Datatype is varchar.
end_station_id: Id of the starting station of the Bike ride. Datatype is varchar.
bike_id: Id of the bike used in the ride. Datatype is varchar.
duration: Duration of the ride in seconds. Datatype is Integer.
user_type: The type of User (either: Customer or Subscriber). Datatype is Vachar.
member_birth_year: Birth year of the rider. Datatype is Integer.
member_gender: Gender of teh rider (Male, Female and Other/Unknown). Datatype is Varchar.

STATIONS
station_id: Id of the station. Datatype is varchar.
station_name: Name of the station. Datatype is varchar.
station_lattitude: Latitude of teh station. Datatype is float.
station_longitude: Longitude of teh station. Datatype is float.

BIKES
bike_id: Id of bike. Datatype is varchar.

TIME
ts: timestamp
hour: hour in the timestamp. Datatype is Integer.
day: day in the timestamp. Datatype is Integer.
week: week derived from timestamp.Datatype is Integer.
month: month in timestamp. Datatype is Integer.
year: year in timestamp. Datatype is Integer.
weekday: weekday represented by the timestamp. Datatype is varchar.

#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.
Tools used was S3 to hold data, Redshift for creating dimension and fact tables and psycopg2 was the library used in
python to programmatically create tables.

* Propose how often the data should be updated and why.
The data should be updated monthly as the number of rides don't generate a lot of data in a day.

* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x
 Redshift can still handle 100x times data as we can use powerful clusters.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 We can use Airflow for scheduling the updation of data daily at 7 am.
 * The database needed to be accessed by 100+ people.
 We need to authenticate user for using tables we created in redshift