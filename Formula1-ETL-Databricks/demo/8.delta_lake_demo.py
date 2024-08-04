# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location "/mnt/formula1projectadlsgen2/demo";

# COMMAND ----------

results_df = spark.read.option("inferSchema", "true") \
    .json("/mnt/formula1projectadlsgen2/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed_delta;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula1projectadlsgen2/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_demo.results_external 
# MAGIC using delta
# MAGIC location "/mnt/formula1projectadlsgen2/demo/results_external";

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external;

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/formula1projectadlsgen2/demo/results_external")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").clusterBy("constructorId").saveAsTable("f1_demo.results_managed_partition_delta")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_managed_partitionby_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_managed_partitionby_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed_partitionby_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.results_managed_partitionby_delta
# MAGIC     set points = 11 - position
# MAGIC where position <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed_partitionby_delta;

# COMMAND ----------

df = spark.read.format("delta").load("/mnt/formula1projectadlsgen2/demo/results_managed_partition_delta")

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1projectadlsgen2/demo/results_managed_partition_delta")
deltaTable.update("position <= 10", {"points": "21-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.results_managed_partitionby_delta
# MAGIC where position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed_partitionby_delta;

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1projectadlsgen2/demo/results_managed_partitionby_delta")
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed_partitionby_delta;