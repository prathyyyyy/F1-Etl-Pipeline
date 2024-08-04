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

from pyspark.sql.types import StructField, StructType, StringType, DoubleType,IntegerType, DateType
from pyspark.sql.functions import col, current_timestamp, to_timestamp, concat, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Circuit csv File

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1projectadlsgen2/raw

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Optimize performance with caching

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read the csv using spark df reader

# COMMAND ----------

races_schema = StructType(
    fields=[
        StructField("raceId", IntegerType(), False),
        StructField("year", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("circuitId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("date", DateType(), True),
        StructField("time", StringType(), True),
        StructField("url", StringType(), True)
    ]
)

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/{file_date}/races.csv",
                             header=True, schema= races_schema)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Select and Rename Required Columns

# COMMAND ----------

races_selected_df = races_df.select(
    col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"),
    col("name"),col("date"), col("time")
)

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Add Ingestion Date To The DataFrame

# COMMAND ----------

races_final_df = add_ingestion_date(races_selected_df)\
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss"))\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date",lit(file_date))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Write Data To Parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").option("mergeSchema", "true").format("delta")\
    .clusterBy("race_year").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")