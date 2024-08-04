# Databricks notebook source
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

# MAGIC %md
# MAGIC ## Step-1. Import Necessary Libraries

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,concat,lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Constructors json file

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-2. Optimize performance with caching on Databricks

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", True)
spark.conf.set("spark.sql.adaptive.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-3. Read the JSON File Using The Spark df

# COMMAND ----------

name_schema = StructType(fields=
    [
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True)
    ]
)

driver_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(),True),
    StructField("code", StringType(), True),
    StructField("name", name_schema, True),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

driver_df = spark.read.json(f"{raw_folder_path}/{file_date}/drivers.json",
                                 schema= driver_schema)

# COMMAND ----------

display(driver_df)

# COMMAND ----------

driver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-4. Remove and Rename Required Columns

# COMMAND ----------

driver_withcolumns_df = add_ingestion_date(driver_df)\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("driverRef", "driver_ref")\
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date",lit(file_date))

# COMMAND ----------

display(driver_withcolumns_df)

# COMMAND ----------

driver_final_df = driver_withcolumns_df.drop(col("url"))

# COMMAND ----------

display(driver_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step-5. Write Output to Parquet File 

# COMMAND ----------

driver_final_df.write.mode("overwrite").format("delta")\
    .option("mergeSchema", "true").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")