# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace temp view dominant_teams
# MAGIC as
# MAGIC select team_name, 
# MAGIC        count(1) as total_races,
# MAGIC        sum(calculated_points) as total_points,
# MAGIC        avg(calculated_points) as avg_points,
# MAGIC        rank() over(order by avg(calculated_points) desc) as team_rank
# MAGIC   from f1_presentation.calculated_race_results
# MAGIC group by team_name
# MAGIC having(count(1) > 100)
# MAGIC order by avg_points desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC select team_name, race_year,
# MAGIC        count(1) as total_races,
# MAGIC        sum(calculated_points) as total_points,
# MAGIC        avg(calculated_points) as avg_points
# MAGIC   from f1_presentation.calculated_race_results
# MAGIC   where team_name in (select team_name from dominant_teams where team_rank <= 5)
# MAGIC group by team_name, race_year
# MAGIC order by avg_points desc 