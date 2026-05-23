# ============================================================
# PySpark Large DataFrame — Memory Optimization Patterns
# Platform : AWS EMR (PySpark)
# Purpose  : Demonstrates strategies to handle large DataFrames
#            (~8GB+) without executor OOM (Exit code 137)
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("LargeDataFrameOptimization") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.executor.memory", "16g") \
    .config("spark.executor.memoryOverhead", "4g") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

INPUT_TABLE  = "team_postpaid_business.large_snapshot_table"
HDFS_STAGING = "hdfs:///tmp/staging/large_export/"
S3_OUTPUT    = "s3://analytics-bucket/postpaid/large_export/"
PARTITION_DT = "2026-04-30"

# ── Strategy 1: Push filters early (predicate pushdown) ──────
# Always filter on partition key FIRST before any joins/aggs
# This avoids loading the full table into memory

df_filtered = spark.sql(f"""
    SELECT *
    FROM {INPUT_TABLE}
    WHERE dl_last_updated = '{PARTITION_DT}'          -- partition pruning
      AND status != 'TEST'                            -- further row reduction
      AND product_code IN ('PP_DELITE','PP_LITE','PP_MINI')
""")

print(f"Filtered row count: {df_filtered.count():,}")

# ── Strategy 2: Select only required columns early ───────────
# Drop unused columns before any shuffle operation
REQUIRED_COLS = ["lead_id", "user_id", "product_code", "status",
                 "current_stage", "created_at"]

df_slim = df_filtered.select(*REQUIRED_COLS)

# ── Strategy 3: Repartition before heavy operations ──────────
# Default 200 partitions can cause large per-partition sizes
# Rule of thumb: ~128MB per partition
df_repartitioned = df_slim.repartition(600, "product_code")

# ── Strategy 4: Cache selectively ────────────────────────────
# Only cache DataFrames that are reused multiple times
df_repartitioned.cache()
df_repartitioned.count()  # trigger cache materialization

# ── Strategy 5: Write to HDFS first, then copy to S3 ─────────
# Direct S3 writes from EMR can cause OOM due to S3 commit protocol
# Writing to HDFS first is more memory-efficient

print("Writing to HDFS staging...")
df_repartitioned.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(HDFS_STAGING)

# Copy from HDFS to S3 using distcp (run via subprocess or EMR step)
# hadoop distcp hdfs:///tmp/staging/large_export/ s3://analytics-bucket/...
print(f"HDFS write complete. Run distcp to copy to: {S3_OUTPUT}")

# ── Strategy 6: Process in chunks if still OOM ───────────────
# Split large DataFrame by a partition key and process each chunk

PRODUCT_VARIANTS = ["PP_DELITE", "PP_LITE", "PP_MINI"]

for variant in PRODUCT_VARIANTS:
    print(f"Processing chunk: {variant}")
    df_chunk = df_filtered.filter(F.col("product_code") == variant)

    df_chunk.coalesce(4).write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(S3_OUTPUT + f"variant={variant}/")

    print(f"  ✓ Written: {variant}")

df_repartitioned.unpersist()
print("All chunks written successfully.")
spark.stop()
