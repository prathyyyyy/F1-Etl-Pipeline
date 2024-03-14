# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest circuit.csv file 
# MAGIC ### Step - 1 Read the csv file using spark dataframe reader
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

v_file_date

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/f1dlsa/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields = [
    StructField("circuitId", IntegerType(),False),
    StructField("circuitRef", StringType(),True),
    StructField("name", StringType(),True),
    StructField("location", StringType(),True),
    StructField("country", StringType(),True),
    StructField("lat", DoubleType(),True),
    StructField("lng", DoubleType(),True),
    StructField("alt",IntegerType(),True),
    StructField("url", StringType(),True)
])

# COMMAND ----------

circuit_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv",header = True,schema= circuits_schema)

# COMMAND ----------

type(circuit_df)

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

display(circuit_df.printSchema())

# COMMAND ----------

display(circuit_df.describe().show())

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Select Required column 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuit_df.select(col("circuitId"),col("circuitRef"),col("name"),
                                         col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step-3 Rename the columns as required  
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
    .withColumnRenamed("circuitRef","circuit_ref")\
    .withColumnRenamed("lat","latitude")\
    .withColumnRenamed("lng","longitude")\
    .withColumnRenamed("alt","altitude")\
    .withColumn("data_source",lit(v_data_strore))\
    .withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step-4. Add Ingestion Date to the dataframe 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)
    #.withColumn("env",lit("Production"))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step-5. Write Data to DeltaLake As Parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

circuits_final_df

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")