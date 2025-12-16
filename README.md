# DS-2002 Project 2 Capstone — Dimensional Data Lakehouse (Insurance Billing)

This repo contains an Azure Databricks notebook that builds a small dimensional *lakehouse* (bronze/silver/gold) for an **insurance billing** business process.

## What the notebook does
1. **File source (CSV)**: loads `insurance.csv` from DBFS and creates dimensions:
   - `dim_member`
   - `dim_region`
   - `dim_risk_profile`
   - **`dim_plan` (additional dimension)**

2. **Relational source (Azure SQL / SQL Server)**:
   - loads `dbo.DimDate` and writes it as Delta table `dim_date`.

3. **NoSQL source (MongoDB)**:
   - attempts to load provider reference data to build `dim_provider` (Mongo connectivity may error depending on cluster/network setup, but the notebook includes the integration step).

4. **Streaming mini-batches (Auto Loader)**:
   - splits fact rows into 3 JSON intervals and ingests them as a streaming source into a **Bronze** Delta table.
   - joins to static dimensions in **Silver** and produces **Gold** aggregations.

## How to run
Open the notebook in Databricks and run cells top-to-bottom. Update secrets/env-vars for:
- Azure SQL connection (JDBC)
- MongoDB connection string (if used)

## Deliverables
- Notebook: `DS2002_Project2_Capstone_Lakehouse_FIXED.ipynb`
