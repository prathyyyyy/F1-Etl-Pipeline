# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor

def run_notebook(notebook_name, data_source, file_date):
    return dbutils.notebook.run(notebook_name, 0, {"data_source": data_source, "file_date": file_date})

notebooks_to_run = [
    ("1.ingest_circuits_file", "Ergast-API", "2021-04-18"),
    ("2.ingest_races_file", "Ergast-API", "2021-04-18"),
    ("3.ingest_constructors_file", "Ergast-API", "2021-04-18"),
    ("4.ingest_drivers_file", "Ergast-API", "2021-04-18"),
    ("5.ingest_results_file", "Ergast-API", "2021-04-18"),
    ("6.ingest_pitstop_file", "Ergast-API", "2021-04-18"),
    ("7.ingest_laptimes_file", "Ergast-API", "2021-04-18"),
    ("8.ingest_qualifying_file", "Ergast-API", "2021-04-18")
]

with ThreadPoolExecutor() as executor:
    futures = [executor.submit(run_notebook, nb[0], nb[1], nb[2]) for nb in notebooks_to_run]
    results = [future.result() for future in futures]

for result in results:
    display(result)