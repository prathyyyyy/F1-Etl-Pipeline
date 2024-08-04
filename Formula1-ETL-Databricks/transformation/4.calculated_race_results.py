# Databricks notebook source
dbutils.widgets.text("file_date","2021-03-21")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

spark.sql(f""" 
 create table if not exists f1_presentation.calculated_race_results(
     race_year int,
     team_name string,
     driver_id int,
     driver_name string,
     race_id int,
     position int,
     points int,
     calculated_points int,
     created_date timestamp,
     updated_date timestamp
) using delta""")

# COMMAND ----------

spark.sql(f"""
              CREATE OR REPLACE TEMP VIEW race_result_updated
              AS
              SELECT races.race_year,
                     constructor.name AS team_name,
                     drivers.driver_id,
                     drivers.name AS driver_name,
                     races.race_id,
                     results.position,
                     results.points,
                     11 - results.position AS calculated_points
                FROM f1_processed.results 
                JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
                JOIN f1_processed.constructor ON (results.constructor_id = constructor.constructor_id)
                JOIN f1_processed.races ON (results.race_id = races.race_id)
               WHERE results.position <= 10
                 AND results.file_date = '{file_date}'
""")

# COMMAND ----------

spark.sql(f"""
            MERGE INTO f1_presentation.calculated_race_results tgt
            USING race_result_updated upd
            ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
            WHEN MATCHED THEN
            UPDATE SET tgt.position = upd.position,
                        tgt.points = upd.points,
                        tgt.calculated_points = upd.calculated_points,
                        tgt.updated_date = current_timestamp
            WHEN NOT MATCHED THEN 
                INSERT (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, created_date ) 
                VALUES (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, current_timestamp)
       """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from race_result_updated;