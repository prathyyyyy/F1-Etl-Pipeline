# Databricks notebook source
# MAGIC %sql
# MAGIC drop database if exists f1_processed cascade;

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_processed 
# MAGIC location "/mnt/formula1projectadlsgen2/processed";

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists f1_presentation cascade;

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_presentation
# MAGIC location "/mnt/formula1projectadlsgen2/presentation";