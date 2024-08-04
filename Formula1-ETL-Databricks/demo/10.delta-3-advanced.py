# Databricks notebook source
# MAGIC %md
# MAGIC ## **Delta Advanced**
# MAGIC - History & Versioning
# MAGIC - Time Travel 
# MAGIC - Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM F1_DEMO.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM F1_DEMO.drivers_merge TIMESTAMP AS OF '2024-06-30T10:17:23.000+00:00';

# COMMAND ----------

df = spark.read.format("delta").option("timeStampAsOf", "2024-06-30T10:17:23.000+00:00").load("/mnt/formula1projectadlsgen2/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql 
# MAGIC vacuum f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.drivers_merge timestamp as of "2024-06-30T10:17:23.000+00:00";

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum F1_DEMO.drivers_merge retain 0 hours

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.drivers_merge timestamp as of "2024-06-30T10:17:23.000+00:00";

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from f1_demo.drivers_merge where driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge version as of 6;

# COMMAND ----------

# MAGIC %sql 
# MAGIC merge into f1_demo.drivers_merge as tgt 
# MAGIC using f1_demo.drivers_merge version as of 6 as src
# MAGIC     on (tgt.driverId = src.driverId)
# MAGIC when not matched then 
# MAGIC insert * 

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;