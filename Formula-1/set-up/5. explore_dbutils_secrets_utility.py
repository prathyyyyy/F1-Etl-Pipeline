# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scope',key='formula1dl-secret-key')

# COMMAND ----------

