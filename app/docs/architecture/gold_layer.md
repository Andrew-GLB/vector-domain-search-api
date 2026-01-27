```mermaid
graph TD
    subgraph Storage_Layer ["Gold Layer (Analytics & Metrics)"]
        direction TB
        GOLD[Gold Layer]
        
        %% Table Entities
        F_ASSET_METRICS[fact_asset_metrics]
       
        %% Connections
        GOLD --> F_ASSET_METRICS
        
    end

    style GOLD fill:#f9f,stroke:#333,stroke-width:2px
```