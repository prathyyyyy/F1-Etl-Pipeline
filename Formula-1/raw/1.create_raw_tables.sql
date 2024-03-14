-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create circuits table 

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
  circuitId int,
  name string,
  location string,
  country string,
  lat double,
  lng double,
  alt int,
  url string
)

using csv 
options (path "/mnt/f1dlsa/raw/circuits.csv",header true)

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### create races table

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
  raceId int,
  year int, 
  round int,
  circuitId int, 
  name string,
  date Date, 
  time string,
  url string
)

using csv 
options (path "/mnt/f1dlsa/raw/races.csv",header true)

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## create tables for JSON files 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create Constructor table
-- MAGIC - Single line JSON
-- MAGIC - Simple Structure

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors
(
  constructorId int,
  constructorRef string, 
  name string, 
  nationality string,
  url string
)

using json 
options (path "/mnt/f1dlsa/raw/constructors.json")

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### create drivers table 
-- MAGIC - single line json 
-- MAGIC - complex structure 

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers
(
  driverId int,
  driverRef string, 
  number int, 
  code string, 
  name STRUCT<forename: string,surname: string>,
  dob date,
  nationality string,
  url string
)

using json 
options (path "/mnt/f1dlsa/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create results table 
-- MAGIC - Single line JSON 
-- MAGIC - Simple structure

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results
(
  resultId int,
  raceId int,
  driverId int,
  constructorId int, 
  number int,grid int, 
  position int, 
  positionText string, 
  positionOrder int,
  points int, 
  laps int, 
  time string, 
  milliseconds int,
  fastestLap int, 
  rank int, 
  fastestLapTime string, 
  fastestLapSpeed float, 
  statusId string
)

using json 
options (path "/mnt/f1dlsa/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create pit stops table
-- MAGIC - Multi Line JSON
-- MAGIC - Simple Structure

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops
(
  driverId int, 
  duration string,
  lap int,
  milliseconds int,
  raceId int, 
  stop int,
  time string 
)

using json 
options (path "/mnt/f1dlsa/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create Tables for list of files

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create Lap times table
-- MAGIC - CSV file 
-- MAGIC - Multiple files

-- COMMAND ----------

drop table if exists f1_raw.lap_times; 
create table if not exists f1_raw.lap_times
(
  raceId int,
  driverId int,
  lap int,
  position int, 
  time string,
  milliseconds int
)

using csv 
options (path "/mnt/f1dlsa/raw/lap_times")

-- COMMAND ----------

select count(1) from f1_raw.lap_times

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create Qualifying Table
-- MAGIC - Json File
-- MAGIC - Multiline JSON 
-- MAGIC - Multiple files

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying
(
  constructorId int,
  driverId int, 
  number int,
  position int, 
  q1 string, 
  q2 string, 
  q3 string, 
  qualifyId int, 
  raceId int
)

using json
options(path "/mnt/f1dlsa/raw/qualifying", multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

