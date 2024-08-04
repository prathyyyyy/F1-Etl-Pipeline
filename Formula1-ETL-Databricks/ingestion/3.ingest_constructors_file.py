# Databricks notebook source
# MAGIC %md
# MAGIC ## Step-1. Import Necessary Libraries

# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

dbutils.widgets.text("data_source","")
v_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date","2021-03-21")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Constructors json file

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-2. Optimize performance with caching on Databricks

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3. Read the JSON File Using The Spark df

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.json(f"{raw_folder_path}/{file_date}/constructors.json",
                                 schema= constructors_schema)

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-4. Remove and Rename Required Columns

# COMMAND ----------

constructor_drop_df = constructor_df.drop(col("url"))

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_drop_df)\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("constructorRef", "constructor_ref")\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date",lit(file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-5. Write Output to Parquet File 

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta")\
    .option("mergeSchema", "true").saveAsTable("f1_processed.constructor")

# COMMAND ----------

dbutils.notebook.exit("Success")