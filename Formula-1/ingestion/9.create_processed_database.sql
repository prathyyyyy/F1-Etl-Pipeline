-- Databricks notebook source
create database if not exists f1_processed
location "/mnt/f1dlsa/processed"

-- COMMAND ----------

desc database f1_raw; 