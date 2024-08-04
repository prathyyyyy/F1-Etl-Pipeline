# Databricks notebook source
# MAGIC %run "../includes/config"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") 

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql("select * from v_race_results where race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------

