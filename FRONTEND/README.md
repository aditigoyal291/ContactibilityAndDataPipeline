# MongoDB to Neo4j Sync Pipeline

This project provides a data pipeline that extracts data from MongoDB Atlas and loads it into a Neo4j graph database using Apache Spark.

## Project Structure

```
mongodb_neo4j_sync/
├── README.md
├── requirements.txt
├── config/
│   └── config.py
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── spark/
│   │   ├── __init__.py
│   │   └── session.py
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── extract.py
│   │   ├── transform.py
│   │   └── load.py
│   ├── graph/
│   │   ├── __init__.py
│   │   ├── neo4j_client.py
│   │   └── schema.py
│   └── utils/
│       ├── __init__.py
│       └── logger.py
└── tests/
    ├── __init__.py
    └── test_etl.py
```

## Prerequisites

- Python 3.7+
- Apache Spark
- MongoDB Atlas account
- Neo4j database

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd mongodb_neo4j_sync
```

2. Install the required packages:

```bash
pip install -r requirements.txt
```

3. Update the configuration:
   - Open `config/config.py` and update the MongoDB and Neo4j connection settings

## Usage

Run the pipeline:

```bash
python src/main.py
```

## Data Model

The pipeline creates the following graph data model in Neo4j:

- Nodes:

  - Person (with labels PrimaryApplicant, CoApplicant, Reference based on roles)
  - Loan
  - Company

- Relationships:
  - (Person:PrimaryApplicant)-[HAS_LOAN]->(Loan)
  - (Loan)-[HAS_COAPPLICANT]->(Person:CoApplicant)
  - (Loan)-[HAS_REFERENCE]->(Person:Reference)
  - (Person)-[WORKS_IN]->(Company)

## Best Practices Implemented

1. **Modular Design**:

   - Separation of concerns with distinct modules
   - Clean interface between components

2. **Configuration Management**:

   - Externalized configuration in config.py
   - Environment-specific settings can be easily changed

3. **Error Handling**:

   - Comprehensive try-except blocks
   - Detailed error logging

4. **Logging**:

   - Centralized logging configuration
   - Both console and file logging

5. **Code Quality**:

   - Docstrings for all functions and classes
   - Type hints and consistent style

6. **PySpark Best Practices**:

   - Proper session management
   - Efficient data processing
   - DataFrame transformations over RDDs

7. **Testing**:
   - Test directory structure set up
   - Ready for unit tests

## License

This project is licensed under the MIT License - see the LICENSE file for details.
