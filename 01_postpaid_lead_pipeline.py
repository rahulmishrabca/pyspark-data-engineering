# ============================================================
# PySpark Batch Processing Pipeline — Postpaid Lead Data
# Platform : AWS EMR (PySpark)
# Purpose  : Load, transform, and export postpaid lead data
#            across multiple product variants to S3
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── Init Spark Session ───────────────────────────────────────
spark = SparkSession.builder \
    .appName("PostpaidLeadPipeline") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Config ───────────────────────────────────────────────────
INPUT_TABLE   = "team_postpaid_business.lead_history_snapshot_v3"
ACCOUNTS_TABLE = "team_postpaid_business.old_pp_accounts_data"
OUTPUT_PATH   = "s3://analytics-bucket/postpaid/lead_export/"
PARTITION_DT  = "2026-04-30"
TARGET_MONTHS = ["2026-03", "2026-04"]

# ── Load Data ────────────────────────────────────────────────
print("Loading lead snapshot data...")

df_leads = spark.sql(f"""
    SELECT
        lead_id,
        user_id,
        product_code,
        status,
        current_stage,
        created_at,
        updated_at
    FROM {INPUT_TABLE}
    WHERE dl_last_updated = '{PARTITION_DT}'
      AND date_format(created_at, 'yyyy-MM') IN ({', '.join([f"'{m}'" for m in TARGET_MONTHS])})
""")

df_accounts = spark.sql(f"""
    SELECT
        user_id,
        account_id,
        credit_limit,
        activation_date
    FROM {ACCOUNTS_TABLE}
    WHERE dl_last_updated = '{PARTITION_DT}'
      AND date_format(activation_date, 'yyyy-MM') IN ({', '.join([f"'{m}'" for m in TARGET_MONTHS])})
""")

print(f"Leads loaded     : {df_leads.count():,}")
print(f"Accounts loaded  : {df_accounts.count():,}")

# ── Transform ────────────────────────────────────────────────
print("Applying transformations...")

# Map product codes to readable variant names
df_leads = df_leads.withColumn(
    "product_variant",
    F.when(F.col("product_code") == "PP_DELITE", "Delite")
     .when(F.col("product_code") == "PP_LITE",   "Lite")
     .when(F.col("product_code") == "PP_MINI",   "Mini")
     .otherwise("Other")
)

# Extract month for partitioning
df_leads = df_leads.withColumn(
    "lead_month",
    F.date_format(F.col("created_at"), "yyyy-MM")
)

# Join with accounts to flag converted leads
df_joined = df_leads.join(
    df_accounts.select("user_id", "account_id", "activation_date"),
    on="user_id",
    how="left"
).withColumn(
    "is_converted",
    F.when(F.col("account_id").isNotNull(), 1).otherwise(0)
)

# Derive rejection flag
df_joined = df_joined.withColumn(
    "is_rejected",
    F.when(F.col("status") == "REJECTED", 1).otherwise(0)
)

# ── Aggregate Summary ────────────────────────────────────────
print("Building summary aggregation...")

df_summary = df_joined.groupBy("lead_month", "product_variant").agg(
    F.countDistinct("lead_id").alias("total_leads"),
    F.sum("is_converted").alias("converted_leads"),
    F.sum("is_rejected").alias("rejected_leads"),
    F.round(
        F.sum("is_converted") * 100.0 / F.countDistinct("lead_id"), 2
    ).alias("conversion_pct"),
    F.round(
        F.sum("is_rejected") * 100.0 / F.countDistinct("lead_id"), 2
    ).alias("rejection_pct")
)

df_summary.show(truncate=False)

# ── Export to S3 ─────────────────────────────────────────────
print(f"Writing output to {OUTPUT_PATH}...")

# Coalesce to reduce small file problem
df_summary.coalesce(4).write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(OUTPUT_PATH + "summary/")

df_joined.coalesce(8).write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .partitionBy("lead_month", "product_variant") \
    .parquet(OUTPUT_PATH + "detail/")

print("Export complete.")
spark.stop()
