import logging
import json

logger = logging.getLogger(__name__)

def row_to_dict(row):
    """Convert a PySpark Row to a dictionary safely"""
    if row is None:
        return {}
    
    # First convert Row to dictionary
    if hasattr(row, "asDict"):
        d = row.asDict()
    elif isinstance(row, dict):
        d = row
    else:
        d = dict(row)  # Try direct conversion
    
    # Then sanitize the values
    result = {}
    for k, v in d.items():
        if v is None:
            continue
        elif isinstance(v, (dict, list)):
            result[k] = json.dumps(v)  # Convert complex objects to JSON strings
        elif hasattr(v, "asDict"):  # Handle nested Row objects
            result[k] = json.dumps(row_to_dict(v))
        else:
            result[k] = v
    
    return result

def get_entity_roles(spark, person_id):
    """Determine the roles of a person (primary applicant, co-applicant, reference)"""
    roles = []
    # Check if the view exists first
    tables = spark.catalog.listTables()
    print("Available tables:", [table.name for table in tables])
    
    
    # Check if primary applicant
    is_primary_row = spark.sql(f"""
        SELECT COUNT(*) as count FROM primaryApplicant 
        WHERE personId = '{person_id}'
    """).first()
    if int(row_to_dict(is_primary_row).get("count", 0)) > 0:
        roles.append("PrimaryApplicant")
    
    # Check if co-applicant
    is_coapplicant_row = spark.sql(f"""
        SELECT COUNT(*) as count FROM coapplicant 
        WHERE personId = '{person_id}'
    """).first()
    if int(row_to_dict(is_coapplicant_row).get("count", 0)) > 0:
        roles.append("CoApplicant")
    
    # Check if reference
    is_reference_row = spark.sql(f"""
        SELECT COUNT(*) as count FROM Reference 
        WHERE personId = '{person_id}'
    """).first()
    if int(row_to_dict(is_reference_row).get("count", 0)) > 0:
        roles.append("Reference")
    
    return roles