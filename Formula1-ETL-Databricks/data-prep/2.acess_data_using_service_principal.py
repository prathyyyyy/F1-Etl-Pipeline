# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount Azure Data Lake Gen2 Using Service Principal

# COMMAND ----------

client_id = dbutils.secrets.get(scope="Formula1-Secret-Scope", key="f1-adls-client-id")
tenant_id = dbutils.secrets.get(scope="Formula1-Secret-Scope", key="f1-adls-tenant-id")
client_secret = dbutils.secrets.get(scope="Formula1-Secret-Scope", key="f1-adls-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1projectadlsgen2.dfs.core.windows.net/",
  mount_point = "/mnt/f1dl/demo",
  extra_configs = configs)

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

display(dbutils.fs.ls("/mnt/f1dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/f1dl/demo/"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/"))