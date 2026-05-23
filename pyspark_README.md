# PySpark Data Engineering

PySpark scripts for large-scale **BNPL / Postpaid lending** data processing on AWS EMR.  
All scripts are anonymized and use representative table/column structures.

---

## 📁 Scripts

### 1. `01_postpaid_lead_pipeline.py`
**Postpaid Lead Batch Processing Pipeline**

End-to-end PySpark pipeline that loads lead and account data from Hive tables, applies product variant mapping, joins across datasets, computes conversion and rejection metrics, and writes partitioned output to S3 in Parquet format.

Key techniques: SparkSession config tuning, Hive SQL ingestion, DataFrame joins, conditional columns, groupBy aggregation, coalesce before S3 write, Snappy compression

---

### 2. `02_large_dataframe_optimization.py`
**Large DataFrame Memory Optimization Patterns**

Demonstrates 6 practical strategies for handling large (~8GB+) DataFrames on EMR without executor OOM errors (Exit code 137) — a common production challenge at scale.

Key techniques: Predicate pushdown, early column pruning, repartition tuning, selective caching, HDFS staging before S3 write, chunk-based processing by partition key

---

## 🛠️ Platform & Environment

- **Processing Engine:** Apache Spark (PySpark) on AWS EMR
- **Storage:** AWS S3 (output), HDFS (staging), Hive Metastore (input)
- **File Format:** Parquet with Snappy compression
- **Partition Strategy:** By `dl_last_updated` (input), by `lead_month` + `product_variant` (output)

---

## 💡 Business Context

These pipelines support:
- **Monthly reporting** — aggregating lead and conversion data by product variant
- **Finance data exports** — validated, partitioned datasets handed off to finance teams
- **Large-scale data reliability** — handling multi-GB DataFrames without memory failures

---

## ⚙️ EMR Configuration Reference

```
spark.executor.memory        = 8g–16g (depending on dataset size)
spark.executor.memoryOverhead = 4g
spark.sql.shuffle.partitions = 200–600 (tune to ~128MB per partition)
spark.memory.fraction        = 0.8
```

---

## 📌 Notes

- All table names and S3 paths are anonymized
- Scripts follow production patterns used with Hive-partitioned tables
- OOM handling strategies in Script 2 are based on real EMR production issues
