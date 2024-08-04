# Databricks notebook source
# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize joins in databricks

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "25")
spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"inner")\
    .select(circuits_df.name, circuits_df.location,circuits_df.country,races_df.name,races_df.round)

# COMMAND ----------

display(race_circuits_df)