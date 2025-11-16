"""
Gold Layer - Claims Star Schema
================================
Transforms Silver validated claims into analytics-ready star schema.

Dimensional Model:
- FactClaim: Grain = one row per claim line
- DimMember: SCD Type 1 (current snapshot)
- DimProvider: SCD Type 1 (current snapshot)  
- DimDate: Standard date dimension

Author: Harsha Morram
Pattern: Medallion Architecture - Gold Layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, monotonically_increasing_id,
    year, month, dayofmonth, dayofweek, quarter, weekofyear,
    when, coalesce, sum as spark_sum, count, avg, max as spark_max
)
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("Gold Claims Star Schema").getOrCreate()

# Paths
SILVER_CLAIMS_PATH = "/mnt/silver/healthcare/claims"
SILVER_MEMBERS_PATH = "/mnt/silver/healthcare/members"
SILVER_PROVIDERS_PATH = "/mnt/silver/healthcare/providers"

GOLD_FACT_CLAIM_PATH = "/mnt/gold/healthcare/fact_claim"
GOLD_DIM_MEMBER_PATH = "/mnt/gold/healthcare/dim_member"
GOLD_DIM_PROVIDER_PATH = "/mnt/gold/healthcare/dim_provider"
GOLD_DIM_DATE_PATH = "/mnt/gold/healthcare/dim_date"


def build_dim_date():
    """
    Build a standard date dimension.
    Covers date range from 2020-01-01 to 2030-12-31.
    """
    
    print("Building DimDate...")
    
    from datetime import datetime, timedelta
    
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2030, 12, 31)
    
    date_list = []
    current_date = start_date
    
    while current_date <= end_date:
        date_list.append((current_date,))
        current_date += timedelta(days=1)
    
    date_df = spark.createDataFrame(date_list, ["date_value"])
    
    dim_date = date_df \
        .withColumn("date_key", col("date_value").cast("string").replace("-", "").cast("int")) \
        .withColumn("year", year("date_value")) \
        .withColumn("month", month("date_value")) \
        .withColumn("day", dayofmonth("date_value")) \
        .withColumn("quarter", quarter("date_value")) \
        .withColumn("day_of_week", dayofweek("date_value")) \
        .withColumn("week_of_year", weekofyear("date_value")) \
        .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), True).otherwise(False)) \
        .withColumn("month_name", 
            when(col("month") == 1, "January")
            .when(col("month") == 2, "February")
            .when(col("month") == 3, "March")
            .when(col("month") == 4, "April")
            .when(col("month") == 5, "May")
            .when(col("month") == 6, "June")
            .when(col("month") == 7, "July")
            .when(col("month") == 8, "August")
            .when(col("month") == 9, "September")
            .when(col("month") == 10, "October")
            .when(col("month") == 11, "November")
            .when(col("month") == 12, "December")
        )
    
    dim_date.write \
        .format("delta") \
        .mode("overwrite") \
        .save(GOLD_DIM_DATE_PATH)
    
    spark.sql(f"CREATE TABLE IF NOT EXISTS gold.dim_date USING DELTA LOCATION '{GOLD_DIM_DATE_PATH}'")
    
    print(f"DimDate created: {dim_date.count()} dates")


def build_dim_member():
    """
    Build DimMember from Silver member master.
    SCD Type 1 - current snapshot only.
    """
    
    print("Building DimMember...")
    
    members_df = spark.read.format("delta").load(SILVER_MEMBERS_PATH)
    
    dim_member = members_df.select(
        col("member_id").alias("member_key"),
        col("member_id"),
        col("first_name"),
        col("last_name"),
        col("date_of_birth"),
        col("gender"),
        col("zip_code"),
        col("plan_type"),
        col("coverage_effective_date"),
        col("coverage_end_date"),
        current_timestamp().alias("dim_updated_timestamp")
    )
    
    dim_member = dim_member.withColumn("member_sk", monotonically_increasing_id())
    
    dim_member.write \
        .format("delta") \
        .mode("overwrite") \
        .save(GOLD_DIM_MEMBER_PATH)
    
    spark.sql(f"CREATE TABLE IF NOT EXISTS gold.dim_member USING DELTA LOCATION '{GOLD_DIM_MEMBER_PATH}'")
    
    print(f"DimMember created: {dim_member.count()} members")


def build_dim_provider():
    """
    Build DimProvider from Silver provider master.
    SCD Type 1 - current snapshot.
    """
    
    print("Building DimProvider...")
    
    providers_df = spark.read.format("delta").load(SILVER_PROVIDERS_PATH)
    
    dim_provider = providers_df.select(
        col("provider_id").alias("provider_key"),
        col("provider_id"),
        col("provider_name"),
        col("npi"),
        col("specialty"),
        col("facility_type"),
        col("address_state"),
        col("network_status"),
        current_timestamp().alias("dim_updated_timestamp")
    )
    
    dim_provider = dim_provider.withColumn("provider_sk", monotonically_increasing_id())
    
    dim_provider.write \
        .format("delta") \
        .mode("overwrite") \
        .save(GOLD_DIM_PROVIDER_PATH)
    
    spark.sql(f"CREATE TABLE IF NOT EXISTS gold.dim_provider USING DELTA LOCATION '{GOLD_DIM_PROVIDER_PATH}'")
    
    print(f"DimProvider created: {dim_provider.count()} providers")


def build_fact_claim():
    """
    Build FactClaim from Silver claims joined to dimension surrogate keys.
    """
    
    print("Building FactClaim...")
    
    claims_df = spark.read.format("delta").load(SILVER_CLAIMS_PATH)
    
    dim_member_df = spark.read.format("delta").load(GOLD_DIM_MEMBER_PATH)
    dim_provider_df = spark.read.format("delta").load(GOLD_DIM_PROVIDER_PATH)
    dim_date_df = spark.read.format("delta").load(GOLD_DIM_DATE_PATH)
    
    fact_claim = claims_df \
        .join(
            dim_member_df.select("member_sk", "member_key"),
            claims_df.member_id == dim_member_df.member_key,
            "left"
        ) \
        .join(
            dim_provider_df.select("provider_sk", "provider_key"),
            claims_df.provider_id == dim_provider_df.provider_key,
            "left"
        ) \
        .join(
            dim_date_df.select(col("date_key").alias("service_date_key"), col("date_value")),
            claims_df.service_date == dim_date_df.date_value,
            "left"
        ) \
        .join(
            dim_date_df.select(col("date_key").alias("received_date_key"), col("date_value").alias("received_date_value")),
            claims_df.received_date == dim_date_df.received_date_value,
            "left"
        )
    
    fact_claim = fact_claim.select(
        col("member_sk"),
        col("provider_sk"),
        col("service_date_key"),
        col("received_date_key"),
        col("claim_id"),
        col("claim_line_number"),
        col("billed_amount"),
        col("allowed_amount"),
        col("paid_amount"),
        (col("billed_amount") - col("paid_amount")).alias("member_responsibility"),
        col("procedure_code"),
        col("diagnosis_code"),
        col("place_of_service"),
        col("claim_type"),
        current_timestamp().alias("fact_created_timestamp")
    )
    
    fact_claim = fact_claim.withColumn("claim_sk", monotonically_increasing_id())
    
    print(f"FactClaim records: {fact_claim.count()}")
    
    if DeltaTable.isDeltaTable(spark, GOLD_FACT_CLAIM_PATH):
        print("Merging into existing FactClaim...")
        
        fact_table = DeltaTable.forPath(spark, GOLD_FACT_CLAIM_PATH)
        
        fact_table.alias("target").merge(
            fact_claim.alias("source"),
            "target.claim_id = source.claim_id AND target.claim_line_number = source.claim_line_number"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        
    else:
        print("Creating new FactClaim...")
        
        fact_claim.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("service_date_key") \
            .save(GOLD_FACT_CLAIM_PATH)
    
    spark.sql(f"CREATE TABLE IF NOT EXISTS gold.fact_claim USING DELTA LOCATION '{GOLD_FACT_CLAIM_PATH}'")
    
    print("FactClaim created/updated")


def build_aggregation_tables():
    """
    Build pre-aggregated tables for common queries.
    """
    
    print("Building aggregation tables...")
    
    fact_df = spark.read.format("delta").load(GOLD_FACT_CLAIM_PATH)
    dim_date_df = spark.read.format("delta").load(GOLD_DIM_DATE_PATH)
    
    monthly_claims = fact_df \
        .join(dim_date_df, fact_df.service_date_key == dim_date_df.date_key) \
        .groupBy("year", "month", "member_sk") \
        .agg(
            count("*").alias("claim_count"),
            spark_sum("billed_amount").alias("total_billed"),
            spark_sum("paid_amount").alias("total_paid"),
            avg("billed_amount").alias("avg_claim_amount")
        )
    
    agg_path = "/mnt/gold/healthcare/agg_monthly_claims"
    
    monthly_claims.write \
        .format("delta") \
        .mode("overwrite") \
        .save(agg_path)
    
    spark.sql(f"CREATE TABLE IF NOT EXISTS gold.agg_monthly_claims USING DELTA LOCATION '{agg_path}'")
    
    print("Aggregation tables created")


def optimize_gold_tables():
    """
    Run OPTIMIZE and Z-ORDER on Gold tables for query performance.
    """
    
    print("Optimizing Gold tables...")
    
    spark.sql(f"OPTIMIZE delta.`{GOLD_FACT_CLAIM_PATH}` ZORDER BY (member_sk, provider_sk, service_date_key)")
    spark.sql(f"OPTIMIZE delta.`{GOLD_DIM_MEMBER_PATH}`")
    spark.sql(f"OPTIMIZE delta.`{GOLD_DIM_PROVIDER_PATH}`")
    
    print("Gold tables optimized")


if __name__ == "__main__":
    """
    Execute Gold layer star schema build.
    """
    
    try:
        build_dim_date()
        build_dim_member()
        build_dim_provider()
        build_fact_claim()
        build_aggregation_tables()
        optimize_gold_tables()
        
        print("\n✓ Gold layer star schema build completed successfully")
        
    except Exception as e:
        print(f"\n✗ Gold layer build failed: {str(e)}")
        raise
    
    finally:
        spark.stop()
