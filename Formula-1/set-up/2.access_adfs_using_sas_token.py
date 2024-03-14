# Databricks notebook source
# MAGIC %md
# MAGIC #### Acess Azure Data Lake Using Access Keys 
# MAGIC 1. Set the spark config fs.azure.account.key 
# MAGIC 2. List files from demo customer 
# MAGIC 3. Read data from circuit.csv file

# COMMAND ----------

formula1_sas_token = dbutils.secrets.get(scope ='formula1-scope' , key= "f1demo-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1dlsa.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1dlsa.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1dlsa.dfs.core.windows.net", formula1_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dlsa.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dlsa.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

