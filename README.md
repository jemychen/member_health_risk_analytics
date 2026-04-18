# Member Health Risk Analytics Pipeline

An end-to-end healthcare data pipeline built with Databricks, dbt, Airflow, and Streamlit.

## Tech Stack
- Databricks (Delta Lake)
- dbt (data transformations)
- Apache Airflow (orchestration)
- Streamlit (dashboard)
- Python / PySpark

## Dataset
CMS Medicare Synthetic Claims Data

## Architecture
Raw Data → Databricks (Bronze) → dbt Staging (Silver) → dbt Marts (Gold) → Streamlit Dashboard