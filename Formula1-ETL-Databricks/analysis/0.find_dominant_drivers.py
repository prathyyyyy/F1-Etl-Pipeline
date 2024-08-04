# Databricks notebook source
# MAGIC %sql
# MAGIC select driver_name, 
# MAGIC        count(1) as total_races,
# MAGIC        sum(calculated_points) as total_points,
# MAGIC        avg(calculated_points) as avg_points
# MAGIC   from f1_presentation.calculated_race_results
# MAGIC group by driver_name
# MAGIC having(count(1) > 50)
# MAGIC order by avg_points desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC select driver_name, 
# MAGIC        count(1) as total_races,
# MAGIC        sum(calculated_points) as total_points,
# MAGIC        avg(calculated_points) as avg_points
# MAGIC   from f1_presentation.calculated_race_results
# MAGIC   where (race_year between 2011 and 2020)
# MAGIC group by driver_name
# MAGIC having(count(1) > 50)
# MAGIC order by avg_points desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC select driver_name, 
# MAGIC        count(1) as total_races,
# MAGIC        sum(calculated_points) as total_points,
# MAGIC        avg(calculated_points) as avg_points
# MAGIC   from f1_presentation.calculated_race_results
# MAGIC   where (race_year between 2001 and 2010)
# MAGIC group by driver_name
# MAGIC having(count(1) > 50)
# MAGIC order by avg_points desc 