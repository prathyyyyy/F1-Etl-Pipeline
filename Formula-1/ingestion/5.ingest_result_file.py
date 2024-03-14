# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest results.json file 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step-1. Read the JSON File using spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("param_data_source","")
v_data_store = dbutils.widgets.get("param_data_source")

# COMMAND ----------

dbutils.widgets.text("param_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("param_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId",IntegerType(),False),
                                   StructField("raceId",IntegerType(),True),
                                   StructField("driverId",IntegerType(),True),
                                   StructField("constructorId",IntegerType(),True),
                                   StructField("number",IntegerType(),True),
                                   StructField("grid",IntegerType(),True),
                                   StructField("position",IntegerType(),True),
                                   StructField("positionText",StringType(),True),
                                   StructField("positionOrder",IntegerType(),True),
                                   StructField("points",FloatType(),True),
                                   StructField("laps",IntegerType(),True),
                                   StructField("time",StringType(),True),
                                   StructField("milliseconds",IntegerType(),True),
                                   StructField("fastestLap",IntegerType(),True),
                                   StructField("rank",IntegerType(),True),
                                   StructField("fastestLapTime",IntegerType(),True),
                                   StructField("fastestLapSpeed",FloatType(),True),
                                   StructField("statusId",StringType(),True)
                                   
])

# COMMAND ----------

results_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/results.json",schema=results_schema)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step-2. Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,lit

# COMMAND ----------

results_final_df = results_df.withColumnRenamed("resultId","result_id")\
    .withColumnRenamed("raceId","race_id")\
    .withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumnRenamed("positionText","position_text")\
    .withColumnRenamed("positionOrder","position_order")\
    .withColumnRenamed("fastestLap","fastest_lap")\
    .withColumnRenamed("fastestLapTime","fastest_lap_time")\
    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
    .withColumnRenamed("statusId","status_id")\
    .withColumn("ingestion_date", current_timestamp())\
    .withColumn("data_source",lit(v_data_store))\
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Write to output to processed container in parquet format 

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed_result")):
#         spark.sql(f"alter table f1_processed.result drop if exists partition (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.result")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method -2 

# COMMAND ----------

overwrite_partition(results_final_df,"f1_processed","result","race_id")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.result

# COMMAND ----------

dbutils.notebook.exit("Success")