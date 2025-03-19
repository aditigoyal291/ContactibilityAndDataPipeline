import logging
from py2neo import Graph

from config.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

logger = logging.getLogger(__name__)

class Neo4jClient:
    def __init__(self):
        self.graph = None
        self.connect()
        
    def connect(self):
        """Connect to Neo4j database"""
        try:
            self.graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            logger.info("Connected to Neo4j database")
        except Exception as e:
            logger.error(f"Error connecting to Neo4j: {str(e)}")
            raise
            
    def init_schema(self):
        """Initialize Neo4j schema with constraints and indexes"""
        try:
            # Create constraints for unique IDs
            self.graph.run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.personId IS UNIQUE")
            self.graph.run("CREATE CONSTRAINT IF NOT EXISTS FOR (l:Loan) REQUIRE l.loanId IS UNIQUE")
            self.graph.run("CREATE CONSTRAINT IF NOT EXISTS FOR (c:Company) REQUIRE c.companyId IS UNIQUE")
            
            # Create index on PAN for faster lookups
            self.graph.run("CREATE INDEX IF NOT EXISTS FOR (p:Person) ON (p.pan)")
            
            logger.info("Neo4j schema initialized")
        except Exception as e:
            logger.error(f"Error initializing Neo4j schema: {str(e)}")
            
    def upsert_person(self, person_id, properties, roles=None):
        """Create or update a Person node with the given properties and roles"""
        try:
            if not roles:
                roles = []
                
            # Create labels string
            labels = "Person"
            for role in roles:
                labels += f":{role}"
                
            query = f"""
            MERGE (p:{labels} {{personId: $personId}})
            SET p += $properties
            """
            
            self.graph.run(query, personId=person_id, properties=properties)
            return True
        except Exception as e:
            logger.error(f"Error upserting Person node: {str(e)}")
            return False
            
    def upsert_loan(self, loan_id, properties):
        """Create or update a Loan node with the given properties"""
        try:
            query = """
            MERGE (l:Loan {loanId: $loanId})
            SET l += $properties
            """
            
            self.graph.run(query, loanId=loan_id, properties=properties)
            return True
        except Exception as e:
            logger.error(f"Error upserting Loan node: {str(e)}")
            return False
            
    def upsert_company(self, company_id, properties):
        """Create or update a Company node with the given properties"""
        try:
            query = """
            MERGE (c:Company {companyId: $companyId})
            SET c += $properties
            """
            
            self.graph.run(query, companyId=company_id, properties=properties)
            return True
        except Exception as e:
            logger.error(f"Error upserting Company node: {str(e)}")
            return False
            
    def create_person_loan_relationship(self, person_id, loan_id):
        """Create HAS_LOAN relationship between a Person and a Loan"""
        try:
            query = """
            MATCH (p:Person {personId: $personId})
            MATCH (l:Loan {loanId: $loanId})
            MERGE (p)-[:HAS_LOAN]->(l)
            """
            
            self.graph.run(query, personId=person_id, loanId=loan_id)
            return True
        except Exception as e:
            logger.error(f"Error creating HAS_LOAN relationship: {str(e)}")
            return False
            
    def create_loan_coapplicant_relationship(self, loan_id, person_id):
        """Create HAS_COAPPLICANT relationship between a Loan and a Person"""
        try:
            query = """
            MATCH (l:Loan {loanId: $loanId})
            MATCH (p:Person {personId: $personId})
            MERGE (l)-[:HAS_COAPPLICANT]->(p)
            """
            
            self.graph.run(query, loanId=loan_id, personId=person_id)
            return True
        except Exception as e:
            logger.error(f"Error creating HAS_COAPPLICANT relationship: {str(e)}")
            return False
            
    def create_loan_reference_relationship(self, loan_id, person_id):
        """Create HAS_REFERENCE relationship between a Loan and a Person"""
        try:
            query = """
            MATCH (l:Loan {loanId: $loanId})
            MATCH (p:Person {personId: $personId})
            MERGE (l)-[:HAS_REFERENCE]->(p)
            """
            
            self.graph.run(query, loanId=loan_id, personId=person_id)
            return True
        except Exception as e:
            logger.error(f"Error creating HAS_REFERENCE relationship: {str(e)}")
            return False
            
    def create_person_company_relationship(self, person_id, company_id):
        """Create WORKS_IN relationship between a Person and a Company"""
        try:
            query = """
            MATCH (p:Person {personId: $personId})
            MATCH (c:Company {companyId: $companyId})
            MERGE (p)-[:WORKS_IN]->(c)
            """
            
            self.graph.run(query, personId=person_id, companyId=company_id)
            return True
        except Exception as e:
            logger.error(f"Error creating WORKS_IN relationship: {str(e)}")
            return False