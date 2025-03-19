"""Neo4j client for graph database operations."""

from py2neo import Graph
from config.config import NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
from src.utils.logger import logger

class Neo4jClient:
    """Neo4j client for graph database operations."""
    
    def __init__(self):
        """Initialize Neo4j client."""
        self.graph = None
        self.connect()
    
    def connect(self):
        """Connect to Neo4j database."""
        try:
            self.graph = Graph(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
            logger.info("Connected to Neo4j database")
        except Exception as e:
            logger.error(f"Error connecting to Neo4j: {str(e)}")
            raise
    
    def clear_database(self):
        """Clear all data from the Neo4j database."""
        try:
            self.graph.run("MATCH (n) DETACH DELETE n")
            logger.info("Cleared all data from Neo4j database")
        except Exception as e:
            logger.error(f"Error clearing Neo4j database: {str(e)}")
            raise
    
    def execute_query(self, query, **params):
        """
        Execute a Cypher query with parameters.
        
        Args:
            query: Cypher query string
            **params: Parameters to pass to the query
            
        Returns:
            Result of the query execution
        """
        try:
            return self.graph.run(query, **params)
        except Exception as e:
            logger.error(f"Error executing Neo4j query: {str(e)}")
            logger.debug(f"Query: {query}")
            logger.debug(f"Params: {params}")
            raise
    
    def create_node(self, labels, id_field, id_value, properties):
        """
        Create a node in Neo4j.
        
        Args:
            labels: String of labels to apply to the node
            id_field: Name of the ID field
            id_value: Value of the ID field
            properties: Dictionary of properties for the node
        """
        query = f"""
        MERGE (n:{labels} {{{id_field}: $id_value}})
        SET n += $properties
        """
        
        try:
            self.execute_query(query, id_value=id_value, properties=properties)
        except Exception as e:
            logger.error(f"Error creating node with {id_field}={id_value}: {str(e)}")
            raise
    
    def create_relationship(self, start_label, start_id_field, start_id_value, 
                           end_label, end_id_field, end_id_value, 
                           relationship_type, properties=None):
        """
        Create a relationship between two nodes.
        
        Args:
            start_label: Label of the start node
            start_id_field: Name of the ID field for the start node
            start_id_value: Value of the ID field for the start node
            end_label: Label of the end node
            end_id_field: Name of the ID field for the end node
            end_id_value: Value of the ID field for the end node
            relationship_type: Type of relationship to create
            properties: Dictionary of properties for the relationship (optional)
        """
        if properties is None:
            properties = {}
        
        query = f"""
        MATCH (a:{start_label} {{{start_id_field}: $start_id_value}})
        MATCH (b:{end_label} {{{end_id_field}: $end_id_value}})
        MERGE (a)-[r:{relationship_type}]->(b)
        """
        
        if properties:
            query += "SET r += $properties"
        
        try:
            self.execute_query(
                query, 
                start_id_value=start_id_value, 
                end_id_value=end_id_value,
                properties=properties
            )
        except Exception as e:
            logger.error(f"Error creating relationship: {str(e)}")
            raise