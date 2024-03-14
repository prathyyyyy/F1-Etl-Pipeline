# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake for Project 
# MAGIC

# COMMAND ----------

def mount_adls(storage_acc_name,container_name):
    # get secrets from key vault 
    client_id = dbutils.secrets.get(scope ='formula1-scope' , key= "f1-serviceprincipal-clientid")
    tenant_id =  dbutils.secrets.get(scope ='formula1-scope' , key= "f1-sp-tenantid")
    client_secret =  dbutils.secrets.get(scope ='formula1-scope' , key= "f1-sp-clientsecretId")
    # Set Spark Configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint":f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # Unmount the mount If its already exists
    if any(mount.mountPoint == f"/mnt/{storage_acc_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_acc_name}/{container_name}")


    # Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_acc_name}/{container_name}",
        extra_configs = configs)
    display(dbutils.fs.mounts())    


# COMMAND ----------

mount_adls("f1dlsa","raw")

# COMMAND ----------

mount_adls("f1dlsa","processed")

# COMMAND ----------

mount_adls("f1dlsa","presentation")

# COMMAND ----------

mount_adls("f1dlsa","demo")

# COMMAND ----------

dbutils.fs.ls("mnt/f1dlsa/demo")