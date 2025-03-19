import os

# MongoDB Configuration
MONGO_URI = "mongodb+srv://chinthanamj:Contact1234@cluster0.dttff.mongodb.net/"
DB_NAME = "Plan5"

# Neo4j Configuration
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

# Schedule Configuration
SYNC_INTERVAL_MINUTES = 5

# Collections to sync
COLLECTIONS = {
    "Person": {
        "id_field": "personId",
        "timestamp_field": "updatedAt" 
    },
    "Company": {
        "id_field": "companyId",
        "timestamp_field": "updatedAt"
    },
    "Loan": {
        "id_field": "loanId",
        "timestamp_field": "updatedAt"
    },
    "PrimaryApplicant": {
        "id_field": "personId",
        "timestamp_field": "updatedAt"
    },
    "Coapplicant": {
        "id_field": "personId",
        "timestamp_field": "updatedAt"
    },
    "Reference": {
        "id_field": "personId",
        "timestamp_field": "updatedAt"
    }
}