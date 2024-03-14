# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest races.csv file 
# MAGIC ### Step - 1 Read the csv file using spark dataframe reader
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("param_data_source","")
v_data_strore = dbutils.widgets.get("param_data_source")

# COMMAND ----------

dbutils.widgets.text("param_file_date","2021-03-21")
v_file_data = dbutils.widgets.get("param_file_date")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/f1dlsa/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(fields = [
    StructField("raceId", IntegerType(),False),
    StructField("year", IntegerType(),True),
    StructField("round", IntegerType(),True),
    StructField("circuitId", IntegerType(),False),
    StructField("name", StringType(),True),
    StructField("date", DateType(),True),
    StructField("time", StringType(),True),
    StructField("url",StringType(),True)
])

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/{v_file_data}/races.csv",header = True,schema = races_schema)

# COMMAND ----------

type(races_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

display(races_df.printSchema())

# COMMAND ----------

display(races_df.describe().show())

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp,lit, concat, col

# COMMAND ----------

races_with_timestamp = races_df.withColumn("ingestion_date",current_timestamp())\
    .withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")), "yyyy-MM-dd HH:mm:ss"))\
    .withColumn("data_source",lit(v_data_strore))\
    .withColumn("data_source",lit(v_file_data))

# COMMAND ----------

display(races_with_timestamp)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 3 - Select only the columns required 

# COMMAND ----------

races_selected_df = races_with_timestamp.select(col("raceId").alias("race_id"),col("year").alias("race_year"),col("date").alias("race_date"),
                                                  col("round"),col("circuitId").alias("circuit_id"),col("name"),
                                                  col("ingestion_date"),col("race_timestamp"),col("data_source"))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step-4. Write Data to DeltaLake As Parquet

# COMMAND ----------

races_selected_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")