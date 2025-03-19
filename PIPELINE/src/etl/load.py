import logging
import json
from datetime import datetime
import pytz

from src.etl.transform import row_to_dict, get_entity_roles
from src.spark.session import update_sync_timestamp
from config.config import COLLECTIONS

logger = logging.getLogger(__name__)

def load_persons(spark, person_df, neo4j_client):
    """Load Person nodes into Neo4j"""
    if person_df is None or person_df.count() == 0:
        logger.info("No Person data to load")
        return 0
        
    count = 0
    for row_person in person_df.collect():
        properties = row_to_dict(row_person)
        person_id = properties.get("personId", "")
        
        if not person_id:
            logger.warning("Skipping Person record with missing personId")
            continue
            
        
        # Get roles for this person
        roles = get_entity_roles(spark, person_id)

        display_name = properties.get("name", "Unknown")
        updated_properties={"name":display_name}
        for key,value in properties.items():
            if key != "name":
                updated_properties[key]=value
        
        # Create or update the node
        success = neo4j_client.upsert_person(person_id, updated_properties, roles)
        if success:
            count += 1
            
    logger.info(f"Loaded {count} Person nodes")
    
    # Update the timestamp if any records were processed
    if count > 0:
        update_sync_timestamp(
            spark, 
            "Person", 
            datetime.now(pytz.UTC)
        )
        
    return count

def load_loans(spark, loan_df, neo4j_client):
    """Load Loan nodes into Neo4j"""
    if loan_df is None or loan_df.count() == 0:
        logger.info("No Loan data to load")
        return 0
        
    count = 0
    for row_loan in loan_df.collect():
        properties = row_to_dict(row_loan)
        loan_id = properties.get("loanId", "")
        
        if not loan_id:
            logger.warning("Skipping Loan record with missing loanId")
            continue
        updated_properties={"loanId":loan_id}
        for key,value in properties.items():
            if key != "loanId":
                updated_properties[key]=value
        # logger.info(f"Inserting Loan Node: {json.dumps(updated_properties, indent=2)}")


        # Create or update the node
        success = neo4j_client.upsert_loan(loan_id, updated_properties)
        if success:
            count += 1
            
    logger.info(f"Loaded {count} Loan nodes")
    
    # Update the timestamp if any records were processed
    if count > 0:
        update_sync_timestamp(
            spark, 
            "Loan", 
            datetime.now(pytz.UTC)
        )
        
    return count

def load_companies(spark, company_df, neo4j_client):
    """Load Company nodes into Neo4j"""
    if company_df is None or company_df.count() == 0:
        logger.info("No Company data to load")
        return 0
        
    count = 0
    for row_company in company_df.collect():
        properties = row_to_dict(row_company)
        company_id = properties.get("companyId", "")
        
        if not company_id:
            logger.warning("Skipping Company record with missing companyId")
            continue
            
        # Create or update the node
        success = neo4j_client.upsert_company(company_id, properties)
        if success:
            count += 1
            
    logger.info(f"Loaded {count} Company nodes")
    
    # Update the timestamp if any records were processed
    if count > 0:
        update_sync_timestamp(
            spark, 
            "Company", 
            datetime.now(pytz.UTC)
        )
        
    return count

def load_primary_applicant_relationships(spark, primary_applicant_df, neo4j_client):
    """Load PrimaryApplicant to Loan relationships"""
    if primary_applicant_df is None or primary_applicant_df.count() == 0:
        logger.info("No PrimaryApplicant data to load")
        return 0
        
    relationship_count = 0
    for row in primary_applicant_df.collect():
        record = row_to_dict(row)
        person_id = record.get('personId')
        
        if not person_id:
            continue
            
        # Extract loans array from JSON string
        loans_str = record.get('loans', '[]')
        try:
            loans = json.loads(loans_str)
            if not isinstance(loans, list):
                loans = [loans]  # Convert to list if it's a single value
                
            for loan_id in loans:
                if not loan_id:  # Skip empty loan IDs
                    continue
                    
                # Create HAS_LOAN relationship
                success = neo4j_client.create_person_loan_relationship(person_id, loan_id)
                if success:
                    relationship_count += 1
        except Exception as e:
            logger.error(f"Error processing loans array for primary applicant {person_id}: {e}")
            
    logger.info(f"Created {relationship_count} PrimaryApplicant-Loan relationships")
    
    # Update the timestamp if any records were processed
    if relationship_count > 0:
        update_sync_timestamp(
            spark, 
            "PrimaryApplicant", 
            datetime.now(pytz.UTC)
        )
        
    return relationship_count

def load_coapplicant_relationships(spark, coapplicant_df, neo4j_client):
    """Load CoApplicant relationships"""
    if coapplicant_df is None or coapplicant_df.count() == 0:
        logger.info("No CoApplicant data to load")
        return 0
        
    relationship_count = 0
    for row in coapplicant_df.collect():
        record = row_to_dict(row)
        loan_id = record.get('loanId')
        person_id = record.get('personId')
        
        if not loan_id or not person_id:
            continue
            
        # Create HAS_COAPPLICANT relationship
        success = neo4j_client.create_loan_coapplicant_relationship(loan_id, person_id)
        if success:
            relationship_count += 1
            
    logger.info(f"Created {relationship_count} Loan-CoApplicant relationships")
    
    # Update the timestamp if any records were processed
    if relationship_count > 0:
        update_sync_timestamp(
            spark, 
            "Coapplicant", 
            datetime.now(pytz.UTC)
        )
        
    return relationship_count

def load_reference_relationships(spark, reference_df, neo4j_client):
    """Load Reference relationships"""
    if reference_df is None or reference_df.count() == 0:
        logger.info("No Reference data to load")
        return 0
        
    relationship_count = 0
    for row in reference_df.collect():
        record = row_to_dict(row)
        loan_id = record.get('loanId')
        person_id = record.get('personId')
        
        if not loan_id or not person_id:
            continue
            
        # Create HAS_REFERENCE relationship
        success = neo4j_client.create_loan_reference_relationship(loan_id, person_id)
        if success:
            relationship_count += 1
            
    logger.info(f"Created {relationship_count} Loan-Reference relationships")
    
    # Update the timestamp if any records were processed
    if relationship_count > 0:
        update_sync_timestamp(
            spark, 
            "Reference", 
            datetime.now(pytz.UTC)
        )
        
    return relationship_count

def load_person_company_relationships(spark, person_df, neo4j_client):
    """Load Person-Company relationships"""
    if person_df is None or person_df.count() == 0:
        logger.info("No Person data to load Person-Company relationships")
        return 0
        
    relationship_count = 0
    for row in person_df.collect():
        record = row_to_dict(row)
        person_id = record.get('personId')
        company_id = record.get('companyId')
        
        if not person_id or not company_id:
            continue
            
        # Create WORKS_IN relationship
        success = neo4j_client.create_person_company_relationship(person_id, company_id)
        if success:
            relationship_count += 1
            
    logger.info(f"Created {relationship_count} Person-Company relationships")
    return relationship_count