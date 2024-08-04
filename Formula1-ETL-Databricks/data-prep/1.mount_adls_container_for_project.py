# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount Azure Data Lake Gen2 for project

# COMMAND ----------

def mount_adls(storage_account_name,container_name):
    # get Secrets from vault
    client_id = dbutils.secrets.get(scope="Formula1-Secret-Scope", key="f1-adls-client-id")
    tenant_id = dbutils.secrets.get(scope="Formula1-Secret-Scope", key="f1-adls-tenant-id")
    client_secret = dbutils.secrets.get(scope="Formula1-Secret-Scope", key="f1-adls-client-secret")

    # Set Spark configs
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount using configs
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("formula1projectadlsgen2","raw")

# COMMAND ----------

mount_adls("formula1projectadlsgen2","processed")

# COMMAND ----------


mount_adls("formula1projectadlsgen2","presentation")

# COMMAND ----------

mount_adls("formula1projectadlsgen2","demo")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1projectadlsgen2/demo")

# COMMAND ----------

display(spark.read.csv("/mnt/f1dl/demo/"))