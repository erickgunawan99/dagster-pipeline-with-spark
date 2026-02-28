## Data Pipeline Flow

```mermaid
flowchart TD
    A["Manual Upload Claims JSON → claims bucket"] --> B[(claims_json bucket)]
    P["Manual Upload Policies JSON → policies bucket"] --> Q[(policies_json bucket)]

    subgraph "Dagster Orchestration"
        CS["claim_sensor\n(Monitors claims bucket)"] -.->|File arrives| B
        PS["policy_sensor\n(Monitors policies bucket)"] -.->|File arrives| Q

        CS -->|Triggers| SE["Spark Job - Claims\n(Unnest JSON → Write Parquet)"]
        PS -->|Triggers| SP["Spark Job - Policies\n(Unnest JSON → Write Parquet)"]

        SE -->|Outputs to| F[(claims_parquet bucket)]
        SP -->|Outputs to| G[(policies_parquet bucket)]
    end

    subgraph "Dagster trigger dbt build (DuckDB) after spark finished"
        J["Staging -> stg_claim ← external claims_parquet]
        H["Staging -> stg_policy ← external policies_parquet]

        F --> J
        G --> H

        K["Intermediate (int_claim_details: JOIN stg_claim + stg_policy)"]

        J --> K
        H --> K

        H --> L|Derived from stg_policy| L["Dimensions(dim_beneficiary, dim_asset, dim_policy_holder, dim_policy)"]

        M["Fact(fct_claim ← from int_claim_details)"]
        K --> M
        H --> M
    end

    classDef manual fill:#ffebee,stroke:#333,stroke-width:2px,color:#333
    classDef storage fill:#fffde7,stroke:#333,stroke-width:2px,color:#333
    classDef dagster fill:#e0f7fa,stroke:#333,stroke-width:2px,color:#333
    classDef spark fill:#fff3e0,stroke:#333,stroke-width:2px,color:#333
    classDef dbt fill:#e3f2fd,stroke:#333,stroke-width:2px,color:#333

    class A,P manual
    class B,Q,F,G storage
    class CS,PS,SE,SP,H dagster
    class J,K,L,M dbt
