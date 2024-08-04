# Databricks notebook source
# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("data_source","")
v_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-1. Import Necessary Libraries

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Results json file

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-2. Optimize performance with caching and Adaptive query execution on Databricks

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", True)
spark.conf.set("spark.sql.adaptive.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3. Read the JSON File Using The Spark df

# COMMAND ----------

pitstop_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop",StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pitstop_df = spark.read.json(f"{raw_folder_path}/{file_date}/pit_stops.json",
                                 schema= pitstop_schema, multiLine= True)

# COMMAND ----------

display(pitstop_df)

# COMMAND ----------

pitstop_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-4. Rename Required Columns

# COMMAND ----------

pitstop_final_df = add_ingestion_date(pitstop_df)\
    .withColumnRenamed("resultId", "result_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date",lit(file_date))

# COMMAND ----------

display(pitstop_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-5. Write Output to Parquet File 

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"

merge_delta_data(pitstop_final_df,"f1_processed", "pit_stop", processed_folder_path,
                  merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")