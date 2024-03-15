# Formula-1 ETL pipeline Using Azure Databricks and DataFactory
![azure-databricks-modern-analytics-architecture](https://github.com/prathyyyyy/Microsoft-Azure/assets/97932221/35230bfb-7c1e-482b-b931-4cf06e592198)

## 1. Introduction
This project demonstrates the development of an Extract, Transform, Load (ETL) pipeline for processing Formula 1 data using Azure Databricks and Azure Data Factory. The pipeline ingests raw data from various sources, performs transformations, and loads the processed data into Azure Data Lake Gen2 for further analysis and visualization using Power BI.

## 2. Architecture
The solution architecture utilizes the following Azure services:

- Azure Databricks: For data processing, transformation, and orchestration using notebooks and jobs.
- Azure Data Lake Gen2: The storage layer for storing raw and processed data.
- Azure Data Factory: For orchestrating and scheduling the ETL pipeline and executing Databricks notebooks.
- Power BI: For visualizing the processed data through interactive dashboards.

## 3. Key Features

- Azure Databricks: Utilized for building the ETL pipeline with notebooks, leveraging Databricks utilities, magic commands, and cluster management features.
- Delta Lake: Implemented to ensure ACID transactions, schema enforcement, and time travel capabilities, facilitating a Lakehouse architecture.
- Unity Catalog: Employed for data governance, enabling metadata management, data discovery, lineage, and access control.
- Azure Data Factory: Used for orchestrating Databricks notebooks, managing dependencies, and scheduling pipeline execution.
- Spark (PySpark and SQL): Leveraged for data transformations, including ingestion, filtering, joining, aggregations, and window functions.

## 4. Project Structure
The project structure includes the following components:

- Notebooks: Contains Databricks notebooks for different stages of the ETL pipeline, including data ingestion, transformation, and loading.
- Pipelines: Azure Data Factory pipelines for orchestrating the execution of Databricks notebooks and managing dependencies.
- Scripts: Any auxiliary scripts or code used within the pipeline.
