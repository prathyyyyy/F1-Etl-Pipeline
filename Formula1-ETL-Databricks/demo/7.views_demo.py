# Databricks notebook source
# MAGIC %sql
# MAGIC select current_database();

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view v_race_results
# MAGIC as 
# MAGIC select * from demo.race_results_ext_py_delta
# MAGIC where race_year = 2018;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace global temp view gv_race_results 
# MAGIC as 
# MAGIC select *
# MAGIC   from demo.race_results_ext_py_delta
# MAGIC   where race_year = 2012;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view demo.pv_race_results 
# MAGIC as 
# MAGIC select *
# MAGIC   from demo.race_results_ext_py_delta
# MAGIC   where race_year = 2012;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo;