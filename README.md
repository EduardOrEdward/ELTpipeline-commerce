ELTpipeline-commerce
Data Engineering Project
Overview
ELTpipeline-commerce is a standalone ELT pipeline designed to ingest data from an external API, store and transform it across multiple data layers, and prepare it for analytical visualization.
The project focuses on reliable data delivery, data integrity, and consistency across pipeline stages.
Key Features
API data ingestion and object storage.
Multi-layer data processing (Bronze → Silver → Gold).
Workflow orchestration with Apache Airflow.
Data transformation and quality validation using dbt.
Centralized logging and pipeline monitoring.
Analytical data visualization with Metabase.
Technology Stack
Python · Apache Airflow · PostgreSQL · dbt · Docker · SeaweedFS (S3) · Metabase · GitHub Actions
Architecture
The pipeline follows a multi-layer ELT architecture. Data is ingested from an external API, stored in S3-compatible object storage, loaded into PostgreSQL, transformed through successive data layers, and prepared for analytical consumption.
High-Level Architecture
flowchart TD
    API["External API"]

    S3[("SeaweedFS<br/>S3 Object Storage")]

    Bronze[("PostgreSQL<br/>Bronze")]
    Silver[("PostgreSQL<br/>Silver")]
    Gold[("PostgreSQL<br/>Gold")]

    Metabase["Metabase<br/>Analytics & Visualization"]

    Airflow["Apache Airflow<br/>Workflow Orchestration"]
    DBT["dbt<br/>Transformation & Data Quality"]

    API --> S3
    S3 --> Bronze
    Bronze --> Silver
    Silver --> Gold
    Gold --> Metabase

    Airflow -.-> API
    Airflow -.-> S3
    Airflow -.-> Bronze
    Airflow -.-> Silver
    Airflow -.-> Gold

    DBT -.-> Silver
    DBT -.-> Gold

