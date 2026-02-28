## Data Pipeline Flow

```mermaid
flowchart TD
    A["Manual Data Upload\n(Nested JSON: Claims & Policies)"] -->|Upload to MinIO Buckets| B[(MinIO Buckets\n(claims_json & policies_json))]

    subgraph "Dagster Orchestration"
        C["Dagster Sensors\n(Monitor MinIO Buckets)"] -->|File Detected| D["Dagster Run\n(PipeSubprocessClient +\nPipeS3ContextInjector +\nPipeS3MessageReader)"]
        
        D -->|Hand off Job & Display Logs| E["Spark Job\n(Unpack JSON → Write Parquet)"]
        
        E -->|Claims JSON| F[(Parquet: claims_parquet)]
        E -->|Policies JSON| G[(Parquet: policies_parquet)]
        
        H["Dagster Trigger\n(After Spark Jobs Complete)"] --> I["dbt-DuckDB Run\n(Build Star Schema)"]
    end

    B -.->|Monitored| C

    subgraph "dbt Modeling (DuckDB)"
        J["Staging Models\n(stg_claim: external → claims_parquet\nstg_policy: external → policies_parquet)"] --> K["Intermediate Model\n(int_claim_details: JOIN stg_claim + stg_policy)"]
        
        J -->|From stg_policy| L["Dimension Models\n(dim_beneficiary\n dim_asset\n dim_policy_holder\n dim_policy)"]
        
        K --> M["Fact Model\n(fct_claim: from int_claim_details)"]
    end

    I --> J

    classDef manual fill:#f9d7da,stroke:#333
    classDef storage fill:#fff3cd,stroke:#333
    classDef dagster fill:#d4f4dd,stroke:#333
    classDef spark fill:#ffe5cc,stroke:#333
    classDef dbt fill:#cce5ff,stroke:#333

    class A manual
    class B,F,G storage
    class C,D,H dagster
    class E spark
    class I,J,K,L,M dbt
