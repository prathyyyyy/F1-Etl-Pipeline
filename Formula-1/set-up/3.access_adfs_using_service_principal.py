# Databricks notebook source
# MAGIC %md
# MAGIC #### Acess Azure Data Lake using Service Principal 
# MAGIC 1. Register Azure AD application / Service Principal
# MAGIC 2. Generate a secret/ password for the application
# MAGIC 3. Set Spark Config with App/ Client ID, Directory/ Tenant ID & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributer' to the Data lake.

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

display(dbutils.fs.ls("abfss://demo@f1dlsa.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dlsa.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

