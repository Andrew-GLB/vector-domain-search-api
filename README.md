flowchart TD
    subgraph Client_Layer ["External"]
        User["REST Client / Postman"]
    end

    subgraph FastAPI_Application ["Application Layer"]
        direction TB
        R["API: Routes"] --> S["Services: Business Logic"]
        
        subgraph Entities ["Domain vs Data Access"]
            D["Domain: Pydantic Models"]
            DA["Data Access: SQLModels"]
        end
        
        S -.-> D
        S --> DA
    end

    subgraph Infrastructure ["Storage Layer"]
        DA --> DB[("SQLite Database")]
        S --> VDB[("Typesense Vector DB")]
        S --> AI["Google Gemini API"]
    end

    style D fill:#dfd
    style DA fill:#fdd