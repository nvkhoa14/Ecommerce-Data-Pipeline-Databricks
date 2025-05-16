# Ecommerce-Data-Pipeline-Databricks
## Introduction
Starting with data from data.world, loaded  it into Azure DataLake storage **/landing-zone-1** as a income resources. Using Data
Azure Data Factory to load raw data onto /landing-zone-2/ so that Databricks can transform it into 3 medalion levels for futher
analysis.  Once that is done, we will use Azure Synapse analytics to run the SQL queries on top of the transformed data so that
we can find the insights and get visualization. 
## Architecture
![Arhitecture](https://github.com/nvkhoa14/Ecommerce-Data-Pipeline-Databricks/blob/main/img/Architecture.png)
## DEMO
### Load data
1. Use **landing-zone-1** as data resources.
2. Use ***AZURE DATA FACTORY*** copy data from **landing-zone-1** (.csv format) to **landing-zone-2** (parquet format).
![Factroy](https://github.com/nvkhoa14/Ecommerce-Data-Pipeline-Databricks/blob/main/img/Factory.png)

### Transform data using Databricks.
1. Bronze Layer.
Loading raw data from **landing-zone-2** to Delta lake.
2. Silver Layer.
- Format column type in all tables.
- Change value in some columns to make it easier for read and analyse.
- Add column to tables for futher analyse.
3. Gold Layer.
- Create One Big Table with nececsary columns from four tables.
