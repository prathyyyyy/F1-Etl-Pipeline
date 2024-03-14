# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest drivers.json file 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step-1. Read the JSON File using spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("param_data_source","")
v_data_store = dbutils.widgets.get("param_data_source")

# COMMAND ----------

dbutils.widgets.text("param_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("param_file_date")

# COMMAND ----------

v_data_store

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename",StringType(), True),
                                 StructField("surname",StringType(),True)])

# COMMAND ----------

driver_schema = StructType(fields=[StructField("driverId",IntegerType(),False),
                                   StructField("driverRef",StringType(),True),
                                   StructField("number",IntegerType(),True),
                                   StructField("code",StringType(),True),
                                   StructField("name",name_schema),
                                   StructField("dob",DateType(),True),
                                   StructField("nationality",StringType(),True),
                                   StructField("url",StringType(),True)
])

# COMMAND ----------

drivers_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/drivers.json",schema=driver_schema)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step-2. Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp,lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_id")\
    .withColumnRenamed("driverRef", "driver_ref")\
    .withColumn("ingestion_date",current_timestamp())\
    .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))\
    .withColumn("data_source",lit(v_data_store))\
    .withColumn("data_source",lit(v_file_date))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step-3. Remove unwanted columns
# MAGIC

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Write to output to processed container in parquet format 

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

