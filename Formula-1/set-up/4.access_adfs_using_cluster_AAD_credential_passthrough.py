# Databricks notebook source
# MAGIC %md
# MAGIC #### Acess Azure Data Lake Using Cluster Scoped Credentials 
# MAGIC 1. Set the spark config fs.azure.account.key in cluster
# MAGIC 2. List files from demo customer 
# MAGIC 3. Read data from circuit.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dlsa.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dlsa.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

