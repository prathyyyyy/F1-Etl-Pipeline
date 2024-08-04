# Databricks notebook source
# MAGIC %md
# MAGIC ## **Transaction Log**

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.drivers_trn_log
# MAGIC (
# MAGIC   driverId int,
# MAGIC   dob date,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   createdDate date,
# MAGIC   updatedDate date
# MAGIC )
# MAGIC using delta 
# MAGIC TBLPROPERTIES(
# MAGIC   'delta.enableTypeWidening' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC desc history f1_demo.drivers_trn_log;

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into f1_demo.drivers_trn_log
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_trn_log;