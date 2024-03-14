# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest constructor.json file 
# MAGIC ### Step - 1 Read the json file using spark dataframe reader
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("param_data_source","")
v_data_strore = dbutils.widgets.get("param_data_source")

# COMMAND ----------

dbutils.widgets.text("param_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("param_file_date")

# COMMAND ----------

v_data_strore

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/f1dlsa/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, url STRING"  

# COMMAND ----------

constructors_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/constructors.json",schema= constructors_schema)

# COMMAND ----------

type(constructors_df)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

display(constructors_df.printSchema())

# COMMAND ----------

display(constructors_df.describe().show())

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step-2 Drop unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_drop_df = constructors_df.drop(col("url"))

# COMMAND ----------

display(constructor_drop_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

constructor_final_df = constructor_drop_df.withColumnRenamed("constructorId","constructor_id") \
    .withColumnRenamed("constructorRef","constructor_ref") \
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("data_source",lit(v_data_strore))\
    .withColumn("data_source",lit(v_data_strore))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step-4. Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")