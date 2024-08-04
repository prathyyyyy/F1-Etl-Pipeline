# Databricks notebook source
from pyspark.sql.functions import count, countDistinct, sum, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter("race_year == 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_df.select(count("race_name").alias("total_race_count")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name").alias("unique_race_count")).show()

# COMMAND ----------

demo_df.select(sum("points").alias("total_points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points").alias("Total Points"), 
                                                        countDistinct("race_name").alias("number_of_races")).show()

# COMMAND ----------

demo_df.groupBy("driver_name")\
    .agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window functions

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("race_year","driver_name")\
    .agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)).show()

# COMMAND ----------

