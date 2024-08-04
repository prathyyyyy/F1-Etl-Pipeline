# Databricks notebook source
# MAGIC %run "../includes/config"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("data_source","")
v_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, DoubleType,IntegerType
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Circuit csv File

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Optimize performance with caching

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read the csv using spark df reader

# COMMAND ----------

circuit_schema = StructType(
    fields=[
        StructField("circuitId", IntegerType(), False),
        StructField("circuitRef", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("alt", IntegerType(), True),
        StructField("url", StringType(), True)
    ]
)

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/{file_date}/circuits.csv",
                             header=True, schema= circuit_schema)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Select and Rename Required Columns

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    col("circuitId").alias("circuit_id"), col("circuitRef").alias("circuit_ref"), col("name"), col("location"),
    col("country"), col("lat").alias("latitude"), col("lng").alias("longitude"), col("alt").alias("altitude"))\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date",lit(file_date)) 

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Add Ingestion Date To The DataFrame

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_selected_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Write Data To Parquet

# COMMAND ----------

# Save data as delta file format with delta type widening which enables schema evolution

circuits_final_df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("f1_processed.circuits") 

# COMMAND ----------

dbutils.notebook.exit("Success")