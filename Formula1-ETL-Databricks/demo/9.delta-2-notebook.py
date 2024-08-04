# Databricks notebook source
drivers_day_1_df = spark.read.option("inferSchema", "true").json("/mnt/formula1projectadlsgen2/raw/2021-03-28/drivers.json")\
    .filter("driverId <= 10")\
    .select("driverId","dob", "name.forename","name.surname")

# COMMAND ----------

display(drivers_day_1_df)

# COMMAND ----------

from pyspark.sql.functions import upper

# COMMAND ----------

drivers_day2_df =  spark.read.option("inferSchema", "true").json("/mnt/formula1projectadlsgen2/raw/2021-03-28/drivers.json")\
    .filter("driverId between 6 and 15")\
    .select("driverId","dob", upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day3_df =  spark.read.option("inferSchema", "true").json("/mnt/formula1projectadlsgen2/raw/2021-03-28/drivers.json")\
    .filter("driverId between 1 and 5 or driverId between 16 and 20")\
    .select("driverId","dob", upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day_1_df.createOrReplaceTempView("drivers_day_1_df")
drivers_day2_df.createOrReplaceTempView("drivers_day_2_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge
# MAGIC (
# MAGIC   driverId INT,
# MAGIC   dob DATE, 
# MAGIC   forename STRING,
# MAGIC   surname STRING, 
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES('delta.enableTypeWidening' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Day 1**

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge as tgt
# MAGIC using drivers_day_1_df as upd
# MAGIC on (upd.driverId = tgt.driverId)
# MAGIC when matched then 
# MAGIC   update set 
# MAGIC       tgt.dob = upd.dob,
# MAGIC       tgt.forename = upd.forename,
# MAGIC       tgt.surname = upd.surname,
# MAGIC       tgt.updatedDate = current_timestamp
# MAGIC when not matched then 
# MAGIC   insert(driverId, dob, forename, surname, createdDate) values (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Day 2**

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge as tgt
# MAGIC using drivers_day_2_df as upd
# MAGIC on (upd.driverId = tgt.driverId)
# MAGIC when matched then 
# MAGIC   update set 
# MAGIC       tgt.dob = upd.dob,
# MAGIC       tgt.forename = upd.forename,
# MAGIC       tgt.surname = upd.surname,
# MAGIC       tgt.updatedDate = current_timestamp
# MAGIC when not matched then 
# MAGIC   insert(driverId, dob, forename, surname, createdDate) values (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1projectadlsgen2/demo/drivers_merge")

(deltaTable.alias("tgt")
 .merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId")
 .whenMatchedUpdate(set={
     "dob": "upd.dob",
     "forename": "upd.forename",
     "surname": "upd.surname",
     "updatedDate": current_timestamp()})
 .whenNotMatchedInsert(values={
     "driverId": "upd.driverId",
     "dob": "upd.dob",
     "forename": "upd.forename",
     "surname": "upd.surname",
     "createdDate": current_timestamp()})
 .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;