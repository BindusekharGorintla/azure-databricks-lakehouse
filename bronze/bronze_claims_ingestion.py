"""
Bronze Layer - Raw Claims Ingestion
====================================
Ingests raw EDI 837 institutional claims from landing zone to Delta Bronze layer.

Key Principles:
- Append-only, never update Bronze
- Schema-on-read with mergeSchema for flexibility  
- Add metadata columns for lineage and debugging
- No business logic or transformations

Author: Harsha Morram
Pattern: Medallion Architecture - Bronze Layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from delta.tables import DeltaTable
import os

# Initialize Spark with Delta Lake support
spark = SparkSession.builder \
    .appName("Bronze Claims Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Configuration - typically from ADF parameters or config file
SOURCE_PATH = "/mnt/landing/claims/raw/*.csv"
BRONZE_PATH = "/mnt/bronze/healthcare/claims"
SOURCE_SYSTEM = "EDI_837_SFTP"
CHECKPOINT_PATH = "/mnt/checkpoints/bronze_claims"

def ingest_claims_to_bronze():
    """
    Read raw claim files from landing zone and append to Bronze Delta table.
    
    Schema Evolution Strategy:
    - mergeSchema=True allows new columns to be added automatically
    - Preserves historical data even when source schema changes
    - Bronze acts as immutable audit trail
    """
    
    print(f"Starting Bronze ingestion from {SOURCE_PATH}")
    
    # Read raw CSV files
    # Note: In production, this might be Parquet, JSON, or direct EDI parsing
    raw_claims_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .csv(SOURCE_PATH)
    
    # Add metadata columns for lineage and debugging
    bronze_claims_df = raw_claims_df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", input_file_name()) \
        .withColumn("source_system", lit(SOURCE_SYSTEM)) \
        .withColumn("bronze_load_id", lit(spark.sparkContext.getConf().get("spark.databricks.job.id", "manual")))
    
    print(f"Records to ingest: {bronze_claims_df.count()}")
    
    # Check if Bronze table exists
    if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        print("Bronze table exists - appending with schema merge")
        
        # Append to existing Delta table with schema evolution
        bronze_claims_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .option("checkpointLocation", CHECKPOINT_PATH) \
            .save(BRONZE_PATH)
        
    else:
        print("Creating new Bronze Delta table")
        
        # Create new Delta table partitioned by ingestion date
        bronze_claims_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("ingestion_timestamp") \
            .option("checkpointLocation", CHECKPOINT_PATH) \
            .save(BRONZE_PATH)
    
    # Register as table for SQL access
    spark.sql(f"CREATE TABLE IF NOT EXISTS bronze.claims USING DELTA LOCATION '{BRONZE_PATH}'")
    
    print("Bronze ingestion completed successfully")
    
    # Return row count for monitoring/alerting
    return bronze_claims_df.count()


def validate_bronze_ingestion():
    """
    Basic validation to ensure Bronze ingestion succeeded.
    More detailed quality checks happen in Silver layer.
    """
    
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    
    # Check 1: No completely null rows (usually indicates parsing failure)
    null_rows = bronze_df.filter(
        bronze_df["claim_id"].isNull() & 
        bronze_df["member_id"].isNull() & 
        bronze_df["provider_id"].isNull()
    ).count()
    
    if null_rows > 0:
        print(f"WARNING: Found {null_rows} rows with all key fields null")
    
    # Check 2: Corrupt record detection
    if "_corrupt_record" in bronze_df.columns:
        corrupt_count = bronze_df.filter(bronze_df["_corrupt_record"].isNotNull()).count()
        if corrupt_count > 0:
            print(f"WARNING: Found {corrupt_count} corrupt records - review before processing")
    
    # Check 3: Latest ingestion stats
    latest_batch = bronze_df.filter(
        bronze_df["ingestion_timestamp"] == bronze_df.selectExpr("max(ingestion_timestamp)").first()[0]
    )
    
    print(f"\nLatest batch statistics:")
    print(f"  - Records ingested: {latest_batch.count()}")
    print(f"  - Source files: {latest_batch.select('source_file').distinct().count()}")
    print(f"  - Ingestion time: {latest_batch.select('ingestion_timestamp').first()[0]}")
    
    return True


if __name__ == "__main__":
    """
    Main execution flow.
    In production, this would be triggered by ADF pipeline after file arrival.
    """
    
    try:
        # Ingest claims to Bronze
        row_count = ingest_claims_to_bronze()
        
        # Run basic validation
        validate_bronze_ingestion()
        
        print(f"\n✓ Bronze ingestion successful - {row_count} records processed")
        
    except Exception as e:
        print(f"\n✗ Bronze ingestion failed: {str(e)}")
        raise  # Re-raise to fail the ADF pipeline
    
    finally:
        spark.stop()


"""
PRODUCTION CONSIDERATIONS:

1. Error Handling:
   - Log failures to monitoring table (e.g., bronze_ingestion_log)
   - Send alerts via ADF webhook to Teams/email
   - Implement retry logic for transient failures

2. Performance:
   - For large files (>1GB), consider using Auto Loader instead of spark.read
   - Partition Bronze by ingestion_date (date part of ingestion_timestamp)
   - Use Photon runtime for faster parsing

3. Security:
   - Source path should reference Azure Key Vault for storage credentials
   - Unity Catalog permissions: bronze schema = read/write for data engineers only
   - Add row-level security if Bronze contains PII/PHI

4. Schema Evolution:
   - Monitor schema changes via Delta table history
   - Alert on unexpected new columns (could indicate data quality issue)
   - Document schema changes in Confluence/wiki

5. Cost Optimization:
   - Set retention policy on Bronze (e.g., 90 days if Silver is trusted source)
   - Use lifecycle policies to move old partitions to Archive storage
   - Optimize on schedule: OPTIMIZE bronze.claims ZORDER BY (claim_id)

6. Testing:
   - Unit test: Mock source files with known schemas
   - Integration test: End-to-end with sample data
   - Data quality test: Validate metadata columns populated correctly
"""
