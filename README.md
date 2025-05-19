# Ecommerce-Data-Pipeline-Databricks
## Introduction
Starting with data from data.world, loaded  it into Azure DataLake storage **/landing-zone-1** as a income resources. Using Data
Azure Data Factory to load raw data onto /landing-zone-2/ so that Databricks can transform it into 3 medalion levels for futher
analysis.  Once that is done, we will use Azure Synapse analytics to run the SQL queries on top of the transformed data so that
we can find the insights and get visualization. 
## Architecture
![Arhitecture](https://github.com/nvkhoa14/Ecommerce-Data-Pipeline-Databricks/blob/main/img/Architecture.png)
## DEMO
### DATA
The dataset have 5 files in ***.csv*** format. Incluce:
- `./data/6M-0K-99K.users.dataset.public.csv` and `./data/users.6M0xxK.2024.public.csv` as **User** data. 
+ Because **User** data is larger than others. I seperated it into 10 chunk files by using [chunk-user-data.ipynb](https://github.com/nvkhoa14/Ecommerce-Data-Pipeline-Databricks/blob/main/data/chunk-user-data.ipynb) and stored it in `./data/user-chunk-data`. 
+ Each chunk will be simulated as a new data comes to the flow (Extract) and the pipline will automically transform and load it.
- `./data/Buyers-repartition-by-country.csv` as **Buyer** data.
- `./data/Comparison-of-Sellers-by-Gender-and-Country.csv` as **Seller** data.
- `./data/Countries-with-Top-Sellers-(Fashion-C2C).csv` as **Country** data.
### Load data
1. Use **landing-zone-1** as data resources.
- Haves 4 folders. Each folder contains ***.csv*** files: **User**, **Buyer**, **Selller**, **Country**.
2. Use ***AZURE DATA FACTORY*** copy data from **landing-zone-1** (.csv format) to **landing-zone-2** (parquet format).
![Factroy](https://github.com/nvkhoa14/Ecommerce-Data-Pipeline-Databricks/blob/main/img/Factory.png)

### Transform data using Databricks.
1. [Bronze Layer](https://github.com/nvkhoa14/Ecommerce-Data-Pipeline-Databricks/blob/main/databricks/Ecommerce%20Bronze%20Layer.ipynb).

- Loading raw data from **landing-zone-2** to Delta lake.

2. [Silver Layer](https://github.com/nvkhoa14/Ecommerce-Data-Pipeline-Databricks/blob/main/databricks/Ecommerce%20Silver%20Layer.ipynb).

- Format column type in all tables.
- Change value in some columns to make it easier for read and analyze.
- Add column to tables for futher analyse.

3. [Gold Layer](https://github.com/nvkhoa14/Ecommerce-Data-Pipeline-Databricks/blob/main/databricks/Ecommerce%20Gold%20Layer.ipynb).

- Create One Big Table with nececsary columns from four tables.
![OBT](https://github.com/nvkhoa14/Ecommerce-Data-Pipeline-Databricks/blob/main/img/OBT.png)
