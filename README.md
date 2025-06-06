# Forecast Blackout: Predicting Power Outages in California

<div align="center">
  </div>

<p align="center">
  <strong>An end-to-end data engineering pipeline to ingest, process, and forecast weather-related power outage risks in California.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Apache Airflow"/>
  <img src="https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white" alt="Snowflake"/>
  <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt"/>
  <img src="https://img.shields.io/badge/Tableau-E97627?style=for-the-badge&logo=tableau&logoColor=white" alt="Tableau"/>
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker"/>
</p>

---

## 📖 Table of Contents

- [Problem Statement](#-problem-statement)
- [Key Features](#-key-features)
- [System Architecture](#-system-architecture)
- [Data Pipeline Overview](#-data-pipeline-overview)
- [Tech Stack](#-tech-stack)
- [Repository Structure](#-repository-structure)
- [Setup & Installation](#-setup--installation)
- [Dashboard Visualizations](#-dashboard-visualizations)
- [Future Work](#-future-work)
- [Team](#-team)
- [Acknowledgments](#-acknowledgments)

---

## 🎯 Problem Statement

[cite_start]California frequently experiences power outages caused by adverse weather conditions like storms and high winds, disrupting public safety, healthcare, and economic activity. [cite_start]The state lacks a robust, centralized early-warning system to predict these weather-related outages proactively. [cite_start]This project, "Forecast Blackout," was developed to fill this gap by creating a data-driven forecasting system that integrates historical outage patterns with real-time and forecasted weather data.

---

## ✨ Key Features

- [cite_start]**Automated Data Ingestion:** Daily, automated extraction of power outage data from California's public data portal and weather data from the Open-Meteo API.
- [cite_start]**Hybrid ETL/ELT Processing:** Utilizes both ETL (for initial data loading) and ELT (for in-warehouse transformations with dbt) for a modular and efficient data pipeline.
- [cite_start]**Idempotent & Reliable Loading:** Employs MERGE (upsert) operations into Snowflake to ensure data consistency and prevent duplicates, making the pipeline reliable for production use.
- [cite_start]**Rule-Based Forecasting:** Implements a persistence model to forecast weather conditions 24 hours ahead and applies rule-based thresholds to flag potential outage risks by county.
- [cite_start]**Interactive Dashboards:** Provides comprehensive and interactive Tableau dashboards to visualize outage forecasts, analyze correlations between weather events and outages, and identify high-risk areas.

---

## 🏗️ System Architecture

[cite_start]The system is designed with five logical layers, ensuring modularity and scalability.

<div align="center">
  <img src="https://raw.githubusercontent.com/puks0618/DW-Project/main/tableau_exports/system_architecture.png" alt="System Architecture Diagram" width="700"/>
  <br>
  [cite_start]<em>Fig 1: System Architecture Diagram </em>
</div>

1.  [cite_start]**Data Source Layer:** Consumes historical outage data from an ArcGIS service and real-time/forecasted weather data from the Open-Meteo API.
2.  [cite_start]**Extraction Layer:** Apache Airflow orchestrates Python scripts to extract data from these sources.
3.  [cite_start]**Storage Layer:** Snowflake serves as the scalable, centralized cloud data warehouse for all raw and transformed data.
4.  [cite_start]**Transformation Layer:** dbt (data build tool) is used for SQL-based transformations within Snowflake, following an ELT paradigm.
5.  [cite_start]**Visualization Layer:** Tableau connects to Snowflake to provide rich, interactive dashboards for end-users.

---

## 🌊 Data Pipeline Overview

The project is orchestrated by multiple Apache Airflow DAGs that manage the entire data lifecycle:

1.  [cite_start]**Outage Data ETL:** A DAG extracts power outage data, transforms coordinates, cleans the data, and loads it into a `RAW.OUTAGE_DATA` table in Snowflake using a staging + merge pattern.
2.  [cite_start]**Weather Data Enrichment:** A second DAG queries outage locations and times from Snowflake, fetches corresponding historical weather data from the Open-Meteo API, and loads it into a `RAW.WEATHER_DATA` table.
3.  **dbt Transformation (ELT):** An Airflow DAG triggers `dbt run` and `dbt test` to transform the raw data into cleaned, modeled tables within Snowflake. [cite_start]This creates staging models and a final `ANALYTICS.INT_OUTAGE_WEATHER_JOINED` table that combines outage and weather data.
4.  [cite_start]**Forecasting & Risk Calculation:** The final DAG extracts data from the joined table, generates a 24-hour persistence forecast for each county, calculates an outage risk flag based on weather thresholds, and loads the results into the `ANALYTICS.COUNTY_WEATHER_FORECASTS` table for visualization.

---

## 🛠️ Tech Stack

| Category                | Technology                                                              |
| ----------------------- | ----------------------------------------------------------------------- |
| **Data Orchestration** | `Apache Airflow`                                                        |
| **Data Warehouse** | `Snowflake`                                                             |
| **Data Transformation** | `dbt (data build tool)`                                                 |
| **Core Programming** | `Python` (Pandas, Pyproj)                                               |
| **Data Visualization** | `Tableau`                                                               |
| **Containerization** | `Docker`                                                                |
| **Data Sources** | `Open-Meteo API`, `ArcGIS Services`                                       |
| **Version Control** | `Git & GitHub`                                                          |

---

## 📁 Repository Structure
