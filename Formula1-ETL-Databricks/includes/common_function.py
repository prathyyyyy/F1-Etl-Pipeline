# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def rearrange_partition(input_df,partition_col):
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_col:
            column_list.append(column_name)
    column_list.append(partition_col)
    print(column_list)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name,table_name, partition_column):
    output_df = rearrange_partition(input_df, partition_column)
    
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").format("parquet").partitionBy(partition_column).saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df,column_name):
    df_row_to_list = input_df.select(column_name).\
        distinct().collect()

    column_value_list = [row[column_name] for row in df_row_to_list]
    return column_value_list

# COMMAND ----------

from delta.tables import DeltaTable

def merge_delta_data(input_df,db_name, table_name,folder_path, merge_condition, partition_column):

    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

    if spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}"):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")

        deltaTable.alias("tgt").merge(
                input_df.alias("src"),
                merge_condition)\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()

    else:
        input_df.write\
            .mode("overwrite")\
            .format("delta")\
            .clusterBy(partition_column)\
            .saveAsTable(f"{db_name}.{table_name}")