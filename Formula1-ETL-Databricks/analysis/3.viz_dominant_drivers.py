# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace temp view dominant_drivers
# MAGIC as 
# MAGIC select driver_name, 
# MAGIC        count(1) as total_races,
# MAGIC        sum(calculated_points) as total_points,
# MAGIC        avg(calculated_points) as avg_points,
# MAGIC        rank() over(order by avg(calculated_points) desc) as driver_rank
# MAGIC   from f1_presentation.calculated_race_results
# MAGIC group by driver_name
# MAGIC having(count(1) > 50)
# MAGIC order by avg_points desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC select driver_name, race_year,
# MAGIC        count(1) as total_races,
# MAGIC        sum(calculated_points) as total_points,
# MAGIC        avg(calculated_points) as avg_points
# MAGIC   from f1_presentation.calculated_race_results
# MAGIC   where (
# MAGIC     driver_name in (
# MAGIC       select driver_name from dominant_drivers where driver_rank <=10
# MAGIC     )
# MAGIC   )
# MAGIC group by driver_name, race_year
# MAGIC order by race_year,avg_points desc 