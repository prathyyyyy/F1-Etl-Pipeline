# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using Service Principal 
# MAGIC 1. Get client_id, tenant_id, and client_secret from key vault 
# MAGIC 2. Set Spark config with App/Client ID, Directory/ Tenant ID & secret 
# MAGIC 3. Call file  system unity mount to mount the storage 
# MAGIC 4. Explore other file system utilities to mount 

# COMMAND ----------

client_id = dbutils.secrets.get(scope ='formula1-scope' , key= "f1-serviceprincipal-clientid")
tenant_id =  dbutils.secrets.get(scope ='formula1-scope' , key= "f1-sp-tenantid")
client_secret =  dbutils.secrets.get(scope ='formula1-scope' , key= "f1-sp-clientsecretId")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1dlsa.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1dlsa.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1dlsa.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1dlsa.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1dlsa.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint":f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@f1dlsa.dfs.core.windows.net/",
  mount_point = "/mnt/f1dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/f1dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/f1dl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/f1dl/demo")

# COMMAND ----------

