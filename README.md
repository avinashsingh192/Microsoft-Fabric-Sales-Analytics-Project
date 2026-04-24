# End-to-End Sales Analytics Project with Microsoft Fabric

## Introduction

This project demonstrates a complete end-to-end data engineering pipeline built within the Microsoft Fabric ecosystem. The primary objective was to ingest raw sales data from the World Wide Importers sample dataset, process and transform it into a clean star schema data model, and prepare it for interactive business intelligence analysis.

## Technologies Used

* Microsoft Fabric
* Lakehouse & SQL Analytics Endpoint
* Data Pipelines
* PySpark Notebooks
* Spark SQL
* Delta Lake
* Power BI (for Data Modeling)

## Project Pipeline

The project followed a standard data engineering workflow:

1.  **Data Ingestion:** Raw data was ingested from a web source as a .zip archive into the Lakehouse Files section using a Data Pipeline.
2.  **Data Preparation:** A PySpark notebook was used to programmatically unzip the raw data. The extracted Parquet files were then processed into clean, partitioned Delta tables for both the primary fact table (fact_sale) and all dimension tables.
3.  **Data Transformation & Aggregation:** To create business-ready summaries, two different aggregation strategies were demonstrated:
    * **PySpark DataFrame API:** To summarize sales by date and city.
    * **Spark SQL:** To summarize sales by date and employee.
4.  **Data Modeling:** In the SQL Analytics Endpoint, a star schema was created by defining relationships between the fact and dimension tables, making the data ready for intuitive reporting.

## Key Learnings

* This project provided hands-on experience with the full Microsoft Fabric suite, from ingestion to modeling.
* A key insight was understanding the practical differences between using the PySpark DataFrame API and Spark SQL for transformations. I found SQL to be more readable for complex joins, while PySpark offered more flexibility.
* I learned the importance of proper data modeling (creating a star schema) to enable powerful and accurate BI reporting.
* I also gained experience in programmatic data preparation, such as writing a script to unzip files directly within the cloud environment, which was a necessary step not covered in the original tutorial.

## Acknowledgements

* This project was completed by following the excellent and detailed hands-on tutorial by **Rihab Feki**. Her article provided the foundational steps and guidance for this project. You can find the original article here: [Microsoft Fabric — Hands On Project](https://rihab-feki.medium.com/microsoft-fabric-hands-on-project-b4323b6ac550).
* The dataset used is the World Wide Importers sample dataset, provided by Microsoft.
