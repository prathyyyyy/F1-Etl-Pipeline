# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest laptimes.json file 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step-1. Read the JSON File using spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("param_data_source","")
v_data_strore = dbutils.widgets.get("param_data_source")

# COMMAND ----------

dbutils.widgets.text("param_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("param_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                   StructField("driverId",IntegerType(),True),
                                   StructField("lap",IntegerType(),True),
                                   StructField("position",IntegerType(),True),
                                   StructField("time",StringType(),True),
                                   StructField("milliseconds", IntegerType(), True)                                   
])

# COMMAND ----------

laptimes_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/lap_times",schema=lap_times_schema)

# COMMAND ----------

laptimes_df.printSchema()

# COMMAND ----------

display(laptimes_df)

# COMMAND ----------

laptimes_df.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step-2. Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit

# COMMAND ----------

final_df = laptimes_df.withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumn("ingestion_date", current_timestamp())\
    .withColumn("data_source",lit(v_data_strore))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Write to output to processed container in parquet format 

# COMMAND ----------

overwrite_partition(final_df,"f1_processed","lap_times","race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")