# Databricks notebook source
def ing_files(file_name, def_time, data_source,file_date):
    v_result = dbutils.notebook.run(file_name, def_time, {"param_data_source": data_source,"param_file_date":file_date})
    return v_result


# COMMAND ----------

ing_files("1.ingest_circuit_file", 0, "ergast_api","2021-04-18")
ing_files("2.ingest_races_file", 0, "ergast_api","2021-04-18")
ing_files("3.ingest_constructor_file", 0, "ergast_api","2021-04-18")
ing_files("4.ingest_driver_file", 0, "ergast_api","2021-04-18")
ing_files("5.ingest_result_file", 0, "ergast_api","2021-04-18")
ing_files("6.ingest_pit_stops_file", 0, "ergast_api","2021-04-18")
ing_files("7.ingest_lap_times_file", 0, "ergast_api","2021-04-18")
ing_files("8.ingest_qualifying_file", 0, "ergast_api","2021-04-18")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1)
# MAGIC from f1_processed.result
# MAGIC group by race_id
# MAGIC order by race_id desc