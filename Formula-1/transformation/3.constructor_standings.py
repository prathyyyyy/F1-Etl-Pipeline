# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_result")\
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

race_results_df = race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_result")\
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when,count,col

# COMMAND ----------

constructor_standing_df = race_results_df.groupBy("race_year","team")\
    .agg(sum("points").alias("total_points"),
        count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standing_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank 

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))

# COMMAND ----------

final_df = constructor_standing_df.withColumn("rank",rank().over(constructor_rank_spec))

# COMMAND ----------

overwrite_partition(final_df,"f1_presentation_","constructor_standings","race_year")