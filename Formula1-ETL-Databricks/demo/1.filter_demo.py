# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_filter_df = races_df.filter((col("race_year") == 2019) & (col("round") <=5))

# COMMAND ----------

display(races_filter_df)