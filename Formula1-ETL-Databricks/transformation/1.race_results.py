# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "25")
spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step1. Read All Data

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers")\
    .withColumnRenamed("number", "driver_number")\
    .withColumnRenamed("name", "driver_name")\
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructor")\
    .withColumnRenamed("name","team")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")\
    .withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races")\
    .withColumnRenamed("name","race_name")\
    .withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results")\
    .filter(f"file_date = '{file_date}'")\
    .withColumnRenamed("time","race_time")\
    .withColumnRenamed("race_id","result_race_id")\
    .withColumnRenamed("file_date","result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-2. Join the data

# COMMAND ----------

races_circuit_df = races_df.join(circuits_df,races_df.circuit_id==circuits_df.circuit_id,"inner")\
    .select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

display(races_circuit_df)

# COMMAND ----------

race_results_df = results_df.join(races_circuit_df, results_df.result_race_id == races_circuit_df.race_id)\
    .join(drivers_df,results_df.driver_id == drivers_df.driver_id)\
    .join(constructors_df,results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

final_df = race_results_df.select(
    "race_year","race_name","race_date","circuit_location","driver_name", "race_id",
    "driver_number","driver_nationality","team","grid","fastest_lap","race_time","points", "position", "result_file_date")\
    .withColumn("created_date",current_timestamp())\
    .withColumnRenamed("result_file_date","file_date")

# COMMAND ----------

display(final_df.filter("race_year = 2020 and race_name = 'Abu Dhabi Grand Prix'")\
    .orderBy(final_df.points.desc()))

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name and tgt.race_id = src.race_id"
merge_delta_data(final_df, "f1_presentation","race_results", presentation_folder_path, merge_condition,"race_id")

# COMMAND ----------

