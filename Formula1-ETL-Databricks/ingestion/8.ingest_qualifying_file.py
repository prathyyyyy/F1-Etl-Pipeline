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

from pyspark.sql.functions import current_timestamp,lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(),False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read.json(f"{raw_folder_path}/{file_date}/qualifying",
                                 schema= qualifying_schema,multiLine=True)

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-4. Rename Required Columns

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_df)\
    .withColumnRenamed("qualifyId","qualify_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId","constructor_id")\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date",lit(file_date))

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-5. Write Output to Parquet File 

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")