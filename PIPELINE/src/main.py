import sys
import os
import time
import schedule
from datetime import datetime

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# Now use absolute imports
from src.utils.logger import setup_logger
from src.spark.session import create_spark_session
from src.graph.neo4j_client import Neo4jClient
from src.etl.extract import extract_collection
from src.etl.load import (
    load_persons, 
    load_loans, 
    load_companies, 
    load_primary_applicant_relationships,
    load_coapplicant_relationships,
    load_reference_relationships,
    load_person_company_relationships
)
# from src.config.config import SYNC_INTERVAL_MINUTES
from config.config import SYNC_INTERVAL_MINUTES
# Set up logger
logger = setup_logger()

def run_sync_job(incremental=True):
    """Run the synchronization job"""
    start_time = datetime.now()
    logger.info(f"Starting {'incremental' if incremental else 'full'} sync job at {start_time}")
    
    try:
        # Create Spark session
        spark = create_spark_session()
        if not spark:
            logger.error("Failed to create Spark session. Exiting.")
            return
            
        # Connect to Neo4j
        neo4j_client = Neo4jClient()
        neo4j_client.init_schema()
        
        # Extract data from MongoDB
        logger.info("Extracting data from MongoDB...")
        person_df = extract_collection(spark, "Person", incremental)
        company_df = extract_collection(spark, "Company", incremental)
        loan_df = extract_collection(spark, "Loan", incremental)
        primary_applicant_df = extract_collection(spark, "PrimaryApplicant", incremental)
        co_applicant_df = extract_collection(spark, "Coapplicant", incremental)
        reference_df = extract_collection(spark, "Reference", incremental)
        
        # Load nodes
        logger.info("Loading nodes into Neo4j...")
        person_count = load_persons(spark, person_df, neo4j_client)
        loan_count = load_loans(spark, loan_df, neo4j_client)
        company_count = load_companies(spark, company_df, neo4j_client)
        
        # Load relationships
        logger.info("Loading relationships into Neo4j...")
        primary_rel_count = load_primary_applicant_relationships(spark, primary_applicant_df, neo4j_client)
        coapplicant_rel_count = load_coapplicant_relationships(spark, co_applicant_df, neo4j_client)
        reference_rel_count = load_reference_relationships(spark, reference_df, neo4j_client)
        company_rel_count = load_person_company_relationships(spark, person_df, neo4j_client)
        
        # Log summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Sync job completed in {duration:.2f} seconds")
        logger.info(f"Nodes loaded: {person_count} Persons, {loan_count} Loans, {company_count} Companies")
        logger.info(f"Relationships loaded: {primary_rel_count} Primary, {coapplicant_rel_count} CoApplicant, {reference_rel_count} Reference, {company_rel_count} Company")
        
        # Stop the Spark session
        spark.stop()
        
    except Exception as e:
        logger.error(f"Error during sync job: {str(e)}", exc_info=True)
        import sys
        sys.exit(1)

def main():
    """Main function to schedule and run the sync job"""
    logger.info("Starting MongoDB to Neo4j sync service")
    logger.info(f"Sync interval: {SYNC_INTERVAL_MINUTES} minutes")
    
    # Run the job immediately first time (full sync)
    run_sync_job(incremental=False)
    
    # Schedule the job to run at regular intervals
    schedule.every(SYNC_INTERVAL_MINUTES).minutes.do(run_sync_job, incremental=True)
    
    # Keep running until interrupted
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error(f"Service stopped due to error: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()