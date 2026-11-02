ELTpipeline-commerce

Data Engineering Project

Overview

ELTpipeline-commerce is a standalone ELT pipeline designed to ingest data from an external API, store and transform it across multiple data layers, and prepare it for analytical visualization.

The project focuses on reliable data delivery, data integrity, and consistency across pipeline stages.

Key Features

- API data ingestion and object storage.
- Multi-layer data processing (Bronze → Silver → Gold).
- Workflow orchestration with Apache Airflow.
- Data transformation and quality validation using dbt.
- Centralized logging and pipeline monitoring.
- Analytical data visualization with Metabase.

Technology Stack

Python · Apache Airflow · PostgreSQL · dbt · Docker · SeaweedFS (S3) · Metabase · GitHub Actions

Architecture

The pipeline follows a multi-layer ELT architecture. Data is ingested from an external API, stored in S3-compatible object storage, loaded into PostgreSQL, transformed through successive data layers, and prepared for analytical consumption.

High-Level Architecture

flowchart TB
    subgraph SOURCE["DATA SOURCE"]
        API["External API"]
    end

    subgraph STORAGE["OBJECT STORAGE"]
        S3[("SeaweedFS<br/>S3-compatible storage")]
    end

    subgraph PROCESSING["DATA PROCESSING"]
        BRONZE[("PostgreSQL<br/>Bronze layer")]
        SILVER[("PostgreSQL<br/>Silver layer")]
        GOLD[("PostgreSQL<br/>Gold layer")]
    end

    subgraph ANALYTICS["ANALYTICS"]
        MB["Metabase<br/>Dashboards & Visualization"]
    end

    AF["Apache Airflow<br/>Orchestration"]
    DBT["dbt<br/>Transformation & Data Quality"]

    API --> S3
    S3 --> BRONZE
    BRONZE --> SILVER
    SILVER --> GOLD
    GOLD --> MB

    AF -.-> API
    AF -.-> S3
    AF -.-> BRONZE
    AF -.-> DBT

    DBT -.-> SILVER
    DBT -.-> GOLD

    classDef orchestration fill:#e8f0fe,stroke:#4285f4,color:#174ea6
    classDef transformation fill:#e6f4ea,stroke:#34a853,color:#137333

    class AF orchestration
    class DBT transformation

Legend:

- Solid arrows — data flow.
- Dashed arrows — orchestration and transformation responsibilities.