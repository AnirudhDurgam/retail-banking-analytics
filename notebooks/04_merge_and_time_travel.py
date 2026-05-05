#%% md
# # 04 - MERGE Incremental Load + Time Travel
# ### SQL MERGE + Delta time travel queries
#%%

from delta.tables import DeltaTable
from pyspark.sql.types import *
from pyspark.sql.functions import *
import random

NUM_CUSTOMERS = 500
#%%
# ---- GENERATE INCOMING DAILY BATCH ----
import builtins

def daily_batch(n=500):
    rows = []
    for i in range(n):
        rows.append((
            f"TXN{random.randint(1, 50000):08d}",
            f"CUST{random.randint(1, NUM_CUSTOMERS):05d}",
            random.choice(["purchase","withdrawal",
                           "transfer","refund","deposit"]),
            builtins.round(random.uniform(100, 50000), 2),
            "INR",
            random.choice(["Amazon","Swiggy","Flipkart",
                           "Zomato","PhonePe"]),
            random.choice(["Mumbai","Delhi","Hyderabad",
                           "Bangalore","Chennai"]),
            random.choice(["mobile_app","web","UPI","ATM"]),
            1 if random.random() < 0.025 else 0,
            f"2025-01-15 {random.randint(0,23):02d}:"
            f"{random.randint(0,59):02d}:00",
            builtins.round(random.uniform(0, 100), 2)
        ))
    return rows

schema = StructType([
    StructField("transaction_id",        StringType()),
    StructField("customer_id",           StringType()),
    StructField("transaction_type",      StringType()),
    StructField("amount",                DoubleType()),
    StructField("currency",              StringType()),
    StructField("merchant",              StringType()),
    StructField("city",                  StringType()),
    StructField("channel",               StringType()),
    StructField("is_fraud",              IntegerType()),
    StructField("transaction_timestamp", StringType()),
    StructField("fraud_score",            DoubleType())
])

incoming_df = spark.createDataFrame(daily_batch(500), schema=schema) \
    .withColumn("transaction_timestamp",
                to_timestamp("transaction_timestamp",
                             "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("transaction_date",
                to_date("transaction_timestamp")) \
    .withColumn("transaction_hour",
                hour("transaction_timestamp")) \
    .withColumn("amount_bucket",
        when(col("amount") < 500,    "micro")
       .when(col("amount") < 5000,   "small")
       .when(col("amount") < 25000,  "medium")
       .when(col("amount") < 100000, "large")
       .otherwise("high_value")) \
    .withColumn("is_weekend",
        when(dayofweek("transaction_date").isin([1,7]), 1)
        .otherwise(0)) \
    .withColumn("risk_category",
        when(col("fraud_score") >= 75, "high_risk")
       .when(col("fraud_score") >= 40, "medium_risk")
       .otherwise("low_risk")) \
    .withColumn("silver_processed_ts", current_timestamp())

incoming_df.createOrReplaceTempView("incoming_batch")
print(f"Incoming batch: {incoming_df.count()} transactions")

#%%
# ---- COUNT BEFORE MERGE ----
spark.sql("""
    SELECT COUNT(*) AS silver_before_merge
    FROM silver_transactions
""").show()

#%%
# ---- DEDUPLICATE SOURCE ----
spark.sql("""
    CREATE OR REPLACE TEMP VIEW incoming_batch_dedup AS
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY silver_processed_ts DESC) AS rn
        FROM incoming_batch
    )
    WHERE rn = 1
""")

# ---- EXECUTE MERGE ----
spark.sql("""
    MERGE INTO silver_transactions AS target
    USING incoming_batch_dedup AS source
    ON target.transaction_id = source.transaction_id

    WHEN MATCHED THEN
        UPDATE SET
            target.is_fraud            = source.is_fraud,
            target.risk_score          = source.fraud_score,
            target.risk_category       = source.risk_category,
            target.silver_processed_ts = source.silver_processed_ts

    WHEN NOT MATCHED THEN
        INSERT (transaction_id, customer_id, amount, currency, merchant, 
                transaction_type, city, channel, is_fraud, transaction_timestamp,
                transaction_date, transaction_hour, risk_score, amount_bucket,
                is_weekend, risk_category, silver_processed_ts)
        VALUES (source.transaction_id, source.customer_id, source.amount, 
                source.currency, source.merchant, source.transaction_type,
                source.city, source.channel, source.is_fraud, 
                source.transaction_timestamp, source.transaction_date,
                source.transaction_hour, source.fraud_score, source.amount_bucket,
                source.is_weekend, source.risk_category, source.silver_processed_ts)
""")

# ---- COUNT AFTER MERGE ----
spark.sql("""
    SELECT COUNT(*) AS silver_after_merge
    FROM silver_transactions
""").show()
#%%
# ---- VERIFY NEW ROWS FROM JAN 15 2025 ----
spark.sql("""
    SELECT
        transaction_date,
        COUNT(*)              AS transactions,
        SUM(is_fraud)         AS fraud_count,
        ROUND(AVG(amount), 2) AS avg_amount
    FROM silver_transactions
    WHERE transaction_date = '2025-01-15'
    GROUP BY transaction_date
""").show()

# ---- TIME TRAVEL: Version history ----
spark.sql("""
    DESCRIBE HISTORY silver_transactions
""").show(truncate=False)

# ---- TIME TRAVEL: Query version 0 (before MERGE) ----
spark.sql("""
    SELECT COUNT(*) AS row_count_at_version_0
    FROM silver_transactions VERSION AS OF 0
""").show()

#%%
# ---- TIME TRAVEL: Compare version 0 vs current ----
spark.sql("""
    SELECT 'version_0_before_merge' AS snapshot, COUNT(*) AS row_count
    FROM silver_transactions VERSION AS OF 0
    UNION ALL
    SELECT 'current_after_merge', COUNT(*)
    FROM silver_transactions
""").show()

#%%
# ---- AUDIT: Latest records from MERGE ----
spark.sql("""
    SELECT
        transaction_id,
        customer_id,
        amount,
        is_fraud,
        risk_category,
        silver_processed_ts
    FROM silver_transactions
    WHERE transaction_date = '2025-01-15'
    ORDER BY silver_processed_ts DESC
    LIMIT 15
""").show()

#%%
# ---- DELTA TABLE DETAILS ----
spark.sql("""
    DESCRIBE DETAIL silver_transactions
""").show(truncate=False)

print("\nMERGE and time travel complete.")
print("DESCRIBE HISTORY shows every write as a versioned entry.")
print("VERSION AS OF lets you query any past state for audit or rollback.")
