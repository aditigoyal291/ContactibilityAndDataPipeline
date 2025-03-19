from pyspark.sql import SparkSession
import findspark
import logging

from config.config import MONGO_URI, DB_NAME

# Initialize Spark
findspark.init()

logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and return a SparkSession with MongoDB connector"""
    try:
        spark = SparkSession.builder \
            .appName("MongoDB Atlas to Neo4j Pipeline") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .master("local[*]") \
            .getOrCreate()
        
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}", exc_info=True)
        return None

def get_last_sync_timestamp(spark, collection_name):
    """
    Read the last sync timestamp for a collection from a tracking table
    """
    try:
        # Try to read from a tracking table
        df = spark.read.format("mongo") \
            .option("uri", f"{MONGO_URI}{DB_NAME}.sync_metadata") \
            .load()
        
        if df.count() > 0:
            row = df.filter(df.collection_name == collection_name).first()
            if row:
                return row.last_sync_timestamp
    except Exception as e:
        logger.warning(f"Could not get last sync timestamp: {str(e)}")
    
    # Default to epoch start if no timestamp found
    return "1970-01-01T00:00:00.000Z"

def update_sync_timestamp(spark, collection_name, timestamp):
    """
    Update the sync timestamp for a collection in the tracking table
    """
    try:
        # Create a dataframe with the new timestamp
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        from pyspark.sql.functions import lit, current_timestamp
        
        schema = StructType([
            StructField("collection_name", StringType(), True),
            StructField("last_sync_timestamp", StringType(), True)
        ])
        
        # Current timestamp as string in ISO format
        timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        
        data = [(collection_name, timestamp_str)]
        df = spark.createDataFrame(data, schema)
        
        # Write to MongoDB with overwrite mode
        df.write.format("mongo") \
            .option("uri", f"{MONGO_URI}{DB_NAME}.sync_metadata") \
            .mode("append") \
            .save()
            
        logger.info(f"Updated sync timestamp for {collection_name} to {timestamp_str}")
    except Exception as e:
        logger.error(f"Error updating sync timestamp: {str(e)}")