## Data Pipeline Flow

```mermaid
%%{init: {'theme':'base', 'themeVariables': { 'fontSize': '18px' }}}%%

flowchart TD
    A["Manual Upload: Claims JSON → claims bucket"] --> B[(claims_json bucket)]
    P["Manual Upload: Policies JSON → policies bucket"] --> Q[(policies_json bucket)]

    subgraph "Dagster Orchestration"
        CS["claim_sensor (Monitors claims bucket)"] -.->|File arrives| B
        PS["policy_sensor (Monitors policies bucket)"] -.->|File arrives| Q

        CS -->|Triggers| SE["Spark Job - Claims (Unnest JSON → Parquet)"]
        PS -->|Triggers| SP["Spark Job - Policies (Unnest JSON → Parquet)"]

        SE -->|Outputs to| F[(silver_claim parquet bucket)]
        SP -->|Outputs to| G[(silver_policy parquet bucket)]
    end

    subgraph "Dagster Trigger → dbt build (after Spark finished)"
        I["dbt-DuckDB (Build Star Schema)"]
    end

    subgraph "dbt Modeling (DuckDB)"
        J["Staging: stg_claim ← external silver_claim"]
        H2["Staging: stg_policy ← external silver_policy"]

        F --> J
        G --> H2

        K["Intermediate: int_claim_details (JOIN stg_claim + stg_policy)"]

        J --> K
        H2 --> K

        H2 -->|Derived from stg_policy| L["Dimensions: dim_beneficiary, dim_asset, dim_policy_holder, dim_policy"]

        M["Fact: fct_claim ← from int_claim_details"]
        K --> M
        H2 --> M
    end

    I --> J
    I --> H2

    classDef manual fill:#ffebee,stroke:#333,stroke-width:2px,color:#333
    classDef storage fill:#fffde7,stroke:#333,stroke-width:2px,color:#333
    classDef dagster fill:#e0f7fa,stroke:#333,stroke-width:2px,color:#333
    classDef spark fill:#fff3e0,stroke:#333,stroke-width:2px,color:#333
    classDef dbt fill:#e3f2fd,stroke:#333,stroke-width:2px,color:#333

    class A,P manual
    class B,Q,F,G storage
    class CS,PS,SE,SP,H,I dagster
    class J,H2,K,L,M dbt
```

# Prerequisites:
* Docker
* Python
* WSL (for windows)
* DuckDB

# Instructions
* Spin everything up - docker compose up -d
* Go to minio http://localhost:9000 and see if "raw-data", "silver", and "dagster-pipes-metadata" are created
  - "raw-data" is for the dummy nested json files
  - "silver" is for spark output in parquet files
  - "dagster-pipes-metadata" allows dagster (through dagster-pipes library) to establish communication with spark and display the logs in the dagster UI without having dagster acting as a spark driver
* Go to Dagster UI http://localhost:3000
* Go to "sensor" page and activate both sensors
* Generate "claim" and "policy" files and put them into "raw-data" bucket -> python3 data_gen/generate_data.py (this will trigger the sensors)
* If both sensors are triggered and each kicking off the spark job, monitor the dags of both jobs from the Dagster UI
* Fire up duckdb from the terminal to query the tables
  - duckdb storage/insurance_analytics.db
  - Set aws (minio) credentials
  - SELECT ...


