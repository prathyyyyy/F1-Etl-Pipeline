# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database extended demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database();

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC use demo

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.write.format("parquet").saveAsTable("demo.race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC use demo;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.race_results
# MAGIC where race_year = 2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table demo.race_results_sql
# MAGIC as 
# MAGIC select * from  demo.race_results
# MAGIC where race_year = 2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended demo.race_results_sql;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table demo.race_results_sql;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo;

# COMMAND ----------

race_results_df.write \
    .format("parquet") \
    .option("path", f"{presentation_folder_path}/race_results_ext_py") \
    .mode("overwrite") \
    .saveAsTable("demo.race_results_ext_py")

# COMMAND ----------

race_results_df.write \
    .format("delta") \
    .option("path", f"{presentation_folder_path}/race_results_ext_py_delta") \
    .option("clusterBy", "driver_name,race_year") \
    .mode("overwrite") \
    .saveAsTable("demo.race_results_ext_py_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended demo.race_results_ext_py;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo.race_results_ext_sql(
# MAGIC    race_year INT,
# MAGIC    race_name STRING,
# MAGIC    race_date TIMESTAMP, 
# MAGIC    circuit_location STRING,
# MAGIC    driver_name STRING,
# MAGIC    driver_number INT,
# MAGIC    driver_nationality STRING,
# MAGIC    team STRING,
# MAGIC    grid INT,
# MAGIC    fastest_lap INT,
# MAGIC    race_time STRING,
# MAGIC    points FLOAT,
# MAGIC    position INT,
# MAGIC    created_date TIMESTAMP
# MAGIC )
# MAGIC USING PARQUET
# MAGIC LOCATION "/mnt/formula1projectadlsgen2/presentation/race_results_ext_sql";

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo.race_results_ext_sql_delta(
# MAGIC    race_year INT,
# MAGIC    race_name STRING,
# MAGIC    race_date TIMESTAMP, 
# MAGIC    circuit_location STRING,
# MAGIC    driver_name STRING,
# MAGIC    driver_number INT,
# MAGIC    driver_nationality STRING,
# MAGIC    team STRING,
# MAGIC    grid INT,
# MAGIC    fastest_lap INT,
# MAGIC    race_time STRING,
# MAGIC    points FLOAT,
# MAGIC    position INT,
# MAGIC    created_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (driver_name, race_year)
# MAGIC LOCATION "/mnt/formula1projectadlsgen2/presentation/race_results_ext_sql_delta";

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended demo.race_results_ext_sql_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into demo.race_results_ext_sql
# MAGIC select * from demo.race_results_ext_py where race_year = 2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table