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
<img width="1419" height="772" alt="image" src="https://github.com/user-attachments/assets/f09726a7-6739-497f-a150-a2d77146b62e" />
<img width="1391" height="683" alt="image" src="https://github.com/user-attachments/assets/220e3161-1c27-465e-9ac2-87d5b040d7d6" />
<img width="1359" height="750" alt="image" src="https://github.com/user-attachments/assets/a9ef56dd-a44d-4a8d-b40c-a5ca93975f14" />
