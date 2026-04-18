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

Raw Data and Staging in Databricks
<img width="1238" height="693" alt="image" src="https://github.com/user-attachments/assets/b301e296-c5c8-48bd-a7cd-797933f07063" />


Streamlit Dashboard Final Results
<img width="1346" height="359" alt="image" src="https://github.com/user-attachments/assets/701142ef-e00e-4d2e-bf1a-9b48e5387025" />
<img width="1350" height="521" alt="image" src="https://github.com/user-attachments/assets/84694d9d-3fd3-4783-b1cb-e9578cda8ba8" />
<img width="1352" height="500" alt="image" src="https://github.com/user-attachments/assets/9ecbad5d-979b-42b4-bccf-af1841f5cf98" />
<img width="1344" height="520" alt="image" src="https://github.com/user-attachments/assets/3e8e4dd2-042a-4314-9ddd-d0b363c38c16" />
