# ⚡ Forecast Blackout – Data Warehouse Project

> Predicting power outages in California using weather and outage data.

## 🔧 Tech Stack
- **Airflow** for DAG orchestration (ETL, ELT, Forecast)
- **dbt** for transformations in Snowflake
- **Python** for scripts and forecasting
- **Tableau** for dashboards

## 📁 Project Structure
├── ETL/ # ETL Airflow DAGs

├── ELT/ # ELT DAG + dbt models

├── Forecast/ # Weather forecast DAG

├── Tableau_exports/ # Dashboards and images

├── docker-compose.yaml

📊 Dashboards
See Tableau_exports/ for visual insights (outage trends, forecasts, etc.).

🔍 Features
✅ Airflow DAGs for ETL and forecasting

✅ dbt for Snowflake transformations

✅ Weather API ingestion & 3-day forecasts

✅ Counterfactual explanations

✅ Tableau visualization integration

🌱 Future Improvements

Integrate real-time outage data using webhooks or streaming.

Expand dashboard interactivity with parameterized filters.

Model evaluation metrics integration in dashboard.

📌 Authors
Group 4 – Data Warehouse, Spring 2025
