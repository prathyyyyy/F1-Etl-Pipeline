# Databricks notebook source
# MAGIC %md
# MAGIC #### Acess Azure Data Lake Using Access Keys 
# MAGIC 1. Set the spark config fs.azure.account.key 
# MAGIC 2. List files from demo customer 
# MAGIC 3. Read data from circuit.csv file

# COMMAND ----------

formula1_acckey = dbutils.secrets.get(scope='formula1-scope',key='formula1dl-secret-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.f1dlsa.dfs.core.windows.net",
    formula1_acckey
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dlsa.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dlsa.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

