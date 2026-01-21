graph LR
    A[Raw Input: CSV/JSON/PDF] --> B[(Bronze Layer: File Storage)]
    B --> C{Polars ETL}
    C --> D[(Silver Layer: Cleaned Parquet)]
    D --> E{Polars ETL}
    E --> F[(Gold Layer: Star Schema SQL)]
    F --> G[Typesense Vector DB]

graph TD
    subgraph Client_Layer
        User[REST Client / Frontend]
    end

    subgraph FastAPI_Application
        R[API/Routes] --> C[Controllers/Services]
        C --> D[Domain Entities - Pydantic]
        C --> DA[Data Access - SQLModel]
    end

    subgraph Storage_Layer
        DA --> DB[(SQLite/Supabase)]
        DA --> VDB[(Typesense Vector DB)]
    end