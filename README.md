# Retail Banking Transaction Analytics
## Databricks Medallion Architecture Pipeline

### Overview
End-to-end data engineering pipeline built on Databricks Community
Edition demonstrating Medallion Architecture for retail banking
transaction analytics with fraud detection.

### Architecture
Raw Data (Generated)
      ↓
Bronze Delta — raw + ingestion metadata
      ↓
Silver Delta — cleaned, deduplicated, enriched (Spark SQL)
      ↓
Gold Delta — 3 analytical tables (pure Spark SQL)
      ↓
MERGE — incremental daily load
      ↓
Time Travel — audit trail + version history

### Orchestration
Databricks Job with 4 tasks and explicit dependencies:
bronze_ingestion → silver_transformation →
gold_aggregation → merge_and_time_travel

### Tech Stack
- Apache Spark + Spark SQL
- Delta Lake (ACID, MERGE, time travel)
- Databricks Community Edition (Serverless)
- Python (data generation only)

### Dataset
Synthetic retail banking data:
- ~50,000 transactions | 500 customers
- Dirty data: ~8% nulls, duplicates, negatives
- 2% fraud rate | 15 merchants | 10 cities

### Notebooks
| # | Notebook | Purpose |
|---|---|---|
| 01 | setup_and_ingest | Generate data, Bronze Delta |
| 02 | silver_sql | Clean, deduplicate, enrich |
| 03 | gold_sql | 3 Gold analytical tables |
| 04 | merge_and_time_travel | MERGE + audit trail |

### Results
- Bronze: ~51,500 rows (raw including dirty data)
- Silver: ~47,000 rows (after cleaning)
- Gold daily_summary: X rows
- Gold customer_risk: 500 rows
- Gold merchant_fraud: 15 rows

### Key DE Concepts Demonstrated
- Medallion Architecture Bronze → Silver → Gold
- Delta ACID transactions + schema enforcement
- ROW_NUMBER deduplication in Spark SQL
- MERGE INTO for incremental upsert
- Delta time travel VERSION AS OF
- Databricks Jobs pipeline orchestration
- Data quality assertions before Silver write
- Partitioning by transaction_date
