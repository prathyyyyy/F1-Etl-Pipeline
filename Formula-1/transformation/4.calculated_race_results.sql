-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

select * from races;

-- COMMAND ----------

CREATE table f1_presentation_.calculated_race_results
using parquet
as
SELECT races.race_year,
       constructors.name AS team_name,
       drivers.name AS driver_name,
       results.position,
       results.points,
       11 - results.position as calculated_points
  FROM results 
  JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
  JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN f1_processed.races ON (results.race_id = races.race_id)  
  where results.position <= 10;

-- COMMAND ----------

select * from f1_presentation_.calculated_race_results