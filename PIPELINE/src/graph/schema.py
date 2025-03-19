"""Neo4j schema setup and constraints."""

from src.utils.logger import logger

def setup_schema(neo4j_client):
    """
    Set up Neo4j schema with constraints and indexes.
    
    Args:
        neo4j_client: Neo4jClient instance
    """
    try:
        # Create constraints for unique IDs
        neo4j_client.execute_query(
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.personId IS UNIQUE"
        )
        neo4j_client.execute_query(
            "CREATE CONSTRAINT IF NOT EXISTS FOR (l:Loan) REQUIRE l.loanId IS UNIQUE"
        )
        neo4j_client.execute_query(
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Company) REQUIRE c.companyId IS UNIQUE"
        )
        
        # Create index on PAN for faster lookups
        try:
            neo4j_client.execute_query("CREATE INDEX ON :Person(pan)")
            logger.info("Index created on Person.pan")
        except Exception as e:
            # Index might already exist
            logger.warning(f"Note when creating PAN index: {str(e)}")
        
        logger.info("Neo4j schema setup completed")
    except Exception as e:
        logger.error(f"Error setting up Neo4j schema: {str(e)}")
        raise