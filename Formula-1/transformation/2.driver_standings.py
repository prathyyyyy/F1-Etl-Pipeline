# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("param_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("param_file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Find race years for which the data is to be processed

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_result")\
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

race_results_list = spark.read.parquet(f"{presentation_folder_path}/race_result") \
    .filter(f"file_date = '{v_file_date}'")\
    .select("race_year")\
    .distinct()\
    .collect()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df,"race_year")

# COMMAND ----------

from pyspark.sql.functions import sum, when,count,col

# COMMAND ----------

driver_standing_df = race_results_df.groupBy("race_year","driver_name","driver_nationality", "team")\
    .agg(sum("points").alias("total_points"),
        count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank 

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))

# COMMAND ----------

final_df = driver_standing_df.withColumn("rank",rank().over(driver_rank_spec))

# COMMAND ----------

overwrite_partition(final_df,"f1_presentation_","driver_standings","race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation_.driver_standings;