# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.Create Circuits table

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.circuits;
# MAGIC create table if not exists f1_raw.circuits
# MAGIC (
# MAGIC   circuitId int,
# MAGIC   circuitRef string,
# MAGIC   name string,
# MAGIC   location string,
# MAGIC   country string,
# MAGIC   lat double,
# MAGIC   lng double,
# MAGIC   alt int,
# MAGIC   url string
# MAGIC )
# MAGIC using csv
# MAGIC options (path="/mnt/formula1projectadlsgen2/raw/circuits.csv",header = "True")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.create Races Table 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.races;
# MAGIC create table if not exists f1_raw.races
# MAGIC (
# MAGIC   raceId int,
# MAGIC   year int,
# MAGIC   round int,
# MAGIC   circuitId int,
# MAGIC   name string,
# MAGIC   date date,
# MAGIC   time string,
# MAGIC   url string
# MAGIC
# MAGIC )
# MAGIC using csv
# MAGIC options (path="/mnt/formula1projectadlsgen2/raw/races.csv",header = "True")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Tables For json files

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Create Constructors Table**
# MAGIC - Single line json
# MAGIC - Simple Structure

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.constructors;
# MAGIC create table if not exists f1_raw.constructors
# MAGIC (
# MAGIC  constructorId int,
# MAGIC  constructorRef string,
# MAGIC  name string,
# MAGIC  nationality string,
# MAGIC  url string
# MAGIC )
# MAGIC using json
# MAGIC options (path="/mnt/formula1projectadlsgen2/raw/constructors.json",header = "True")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Create Drivers Table**
# MAGIC - Single line json
# MAGIC - Complex Structure

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.drivers;
# MAGIC create table if not exists f1_raw.drivers
# MAGIC (
# MAGIC  driverId int,
# MAGIC  driverRef string,
# MAGIC  number INT,
# MAGIC  code string,
# MAGIC  name Struct<forename:string, surname:string>,
# MAGIC  dob date,
# MAGIC  nationality string,
# MAGIC  url string
# MAGIC
# MAGIC )
# MAGIC using json
# MAGIC options (path="/mnt/formula1projectadlsgen2/raw/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Create results Table**
# MAGIC - Single line json
# MAGIC - Complex Structure

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.results;
# MAGIC create table if not exists f1_raw.results
# MAGIC (
# MAGIC resultId int,
# MAGIC raceId int,
# MAGIC driverId int,
# MAGIC constructorId int,
# MAGIC number int,
# MAGIC grid int,
# MAGIC position int,
# MAGIC positionText string,
# MAGIC positionOrder int,
# MAGIC points int,
# MAGIC laps int,
# MAGIC time string,
# MAGIC milliseconds int,
# MAGIC fastestLap int,
# MAGIC rank int,
# MAGIC fastestLapTime string,
# MAGIC fastestLapSpeed float,
# MAGIC statusId string
# MAGIC )
# MAGIC using json
# MAGIC options (path="/mnt/formula1projectadlsgen2/raw/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### ***Create Pit-Stops Table***
# MAGIC - Multi line json
# MAGIC - Simple Structure

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.pit_stop;
# MAGIC create table if not exists f1_raw.pit_stop
# MAGIC (
# MAGIC raceId int,
# MAGIC driverId int,
# MAGIC stop string,
# MAGIC lap int,
# MAGIC time string,
# MAGIC duration string,
# MAGIC milliseconds int
# MAGIC )
# MAGIC using json
# MAGIC options (path="/mnt/formula1projectadlsgen2/raw/pit_stops.json", multiLine true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ***Create Table - Multi File source***

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.lap_times;
# MAGIC create table if not exists f1_raw.lap_times
# MAGIC (
# MAGIC   raceId int,
# MAGIC   driverId int,
# MAGIC   lap int,
# MAGIC   position int,
# MAGIC   time string,
# MAGIC   milliseconds int
# MAGIC )
# MAGIC using csv
# MAGIC options (path="/mnt/formula1projectadlsgen2/raw/lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.qualifying;
# MAGIC create table if not exists f1_raw.qualifying
# MAGIC (
# MAGIC   qualifyId int,
# MAGIC   raceId int,
# MAGIC   driverId int,
# MAGIC   constructorId int,
# MAGIC   number int,
# MAGIC   position int,
# MAGIC   q1 string,
# MAGIC   q2 string,
# MAGIC   q3 string
# MAGIC )
# MAGIC using json
# MAGIC options (path="/mnt/formula1projectadlsgen2/raw/qualifying", multiLine true)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_raw.qualifying;