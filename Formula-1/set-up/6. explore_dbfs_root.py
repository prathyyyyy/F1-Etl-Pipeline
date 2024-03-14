# Databricks notebook source
# MAGIC %md 
# MAGIC #### Explore DBFS Root 
# MAGIC 1. List all folders in dbfs root 
# MAGIC 2. Interact with DBFS file browser 
# MAGIC 3. Upload file to DBFS root 

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("/FileStore"))

# COMMAND ----------

display(spark.read.csv("/FileStore/circuits.csv"))