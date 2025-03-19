import logging
from datetime import datetime
import pytz
from pyspark.sql.functions import col

from config.config import MONGO_URI, DB_NAME, COLLECTIONS
from src.spark.session import get_last_sync_timestamp

logger = logging.getLogger(__name__)

def extract_collection(spark, collection_name, incremental=False):
    """
    Extract data from MongoDB collection
    Always does a full extract since timestamp fields are not available
    """
    try:
        # Get collection configuration
        collection_config = COLLECTIONS.get(collection_name)
        if not collection_config:
            logger.warning(f"No configuration found for collection {collection_name}")
            return None
            
        # Basic read operation - read all data
        df = spark.read.format("mongo") \
            .option("uri", f"{MONGO_URI}{DB_NAME}.{collection_name}") \
            .load()
            
        # Register for SQL queries
        df.createOrReplaceTempView(collection_name.lower())
        
        # Log the count of records
        count = df.count()
        logger.info(f"Extracted {count} records from {collection_name}")
        
        return df
    except Exception as e:
        logger.error(f"Error extracting collection {collection_name}: {str(e)}")
        return None