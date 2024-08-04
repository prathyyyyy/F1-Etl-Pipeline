# Databricks notebook source
# MAGIC %md
# MAGIC ## Step-1. Import Necessary Libraries

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("data_source","")
v_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-28")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from delta.tables import DeltaTable

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

results_schema = driver_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(),True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(),True),
    StructField("points", FloatType(), True),
    StructField("laps",IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statusId", StringType(), True)
])

# COMMAND ----------

results_df = spark.read.json(f"{raw_folder_path}/{file_date}/results.json",
                                 schema= results_schema)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-4. Remove and Rename Required Columns

# COMMAND ----------

results_drop_df = results_df.drop(col("statusId"))

# COMMAND ----------

results_final_df = add_ingestion_date(results_drop_df)\
    .withColumnRenamed("resultId", "result_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("positionText", "position_text")\
    .withColumnRenamed("positionOrder", "position_order")\
    .withColumnRenamed("fastestLap","fastest_lap")\
    .withColumnRenamed("fastestLapTime","fastest_lap_time")\
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date",lit(file_date))

# COMMAND ----------

results_drop_df = results_final_df.dropDuplicates(["race_id","driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-5. Write Output to Parquet File 

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"alter table f1_processed.results drop if exists partition (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").format("parquet").partitionBy("race_id").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ***Method 2***

# COMMAND ----------

# %sql
# drop table f1_processed.results

# COMMAND ----------

#overwrite_partition(results_final_df, "f1_processed","results","race_id")

# COMMAND ----------

merge_condition = "tgt.result_id == src.result_id and tgt.race_id = src.race_id"

merge_delta_data(results_drop_df,"f1_processed", "results", processed_folder_path,
                  merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")