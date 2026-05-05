#%% md
# # 03 - Gold Layer Aggregation
# ### Three business-ready tables 
#%%
# ---- GOLD TABLE 1: Daily Transaction Summary ----
spark.sql("""
          CREATE OR REPLACE TEMP VIEW gold_daily_sumary_view AS
          SELECT
          transaction_date,
          channel,
          transaction_type,
          COUNT(DISTINCT transaction_id) AS total_transactions,
          COUNT(DISTINCT customer_id) AS unique_customer,
          ROUND(SUM(amount),2) AS total_volume,
          ROUND(AVG(amount),2) AS avg_transaction_value,
          ROUND(MAX(amount),2) AS max_transaction_value,
          SUM(is_fraud) AS fraud_count,
          ROUND(SUM(is_fraud)*100.0/COUNT(*),4) AS fraud_rate_pct,
          current_timestamp() AS gold_processed_ts
          FROM silver_transactions
          GROUP BY transaction_date,channel,transaction_type
""")

spark.table("gold_daily_sumary_view").write.format("delta").mode("overwrite").partitionBy("transaction_date").saveAsTable("gold_daily_summary")

print(f"Gold daily_summary: {spark.table('gold_daily_summary').count()} rows")

spark.sql("""
    SELECT *
    FROM gold_daily_summary
    ORDER BY transaction_date DESC
    LIMIT 5
""").show()



#%%
# ---- GOLD TABLE 2: Customer Risk Profile ----

spark.sql("""
          CREATE OR REPLACE TEMP VIEW gold_customer_risk_view AS
          SELECT 
          t.customer_id,
          c.customer_name,
          c.city,
          c.account_type,
          c.account_balance,
          COUNT(DISTINCT t.transaction_id) AS total_transactions,
          ROUND(SUM(t.amount),2) AS total_spend,
          ROUND(AVG(t.amount),2) AS avg_transaction,
          ROUND(MAX(t.amount),2) AS max_transaction,
          SUM(t.is_fraud) AS fraud_count,
          ROUND(AVG(t.risk_score),2) AS avg_risk_score,
          MAX(t.transaction_date) AS last_transaction_date,
          MIN(t.transaction_date) AS first_transaction_date,
          DATEDIFF(
              MAX(t.transaction_date),
              MIN(t.transaction_date)
          ) AS active_days,
          CASE
            WHEN SUM(t.is_fraud) > 2 THEN 'HIGH_RISK'
            WHEN SUM(t.is_fraud) > 0 THEN 'MEDIUM_RISK'
            WHEN AVG(t.risk_score) > 60 THEN 'WATCH_LIST'
            ELSE 'LOW_RISK'
            END  AS customer_risk_label,
          RANK() OVER(Partition BY c.city ORDER BY SUM(t.amount) DESC) AS spend_rank_in_city,
          current_timestamp() AS gold_processed_ts
          FROM silver_transactions t
          LEFT JOIN silver_customers c
          ON t.customer_id = c.customer_id
          GROUP BY t.customer_id,c.customer_name,c.city,
          c.account_type,c.account_balance
          """)
spark.table("gold_customer_risk_view").write.format("delta").mode("overwrite").saveAsTable("gold_customer_risk")

print(f"Gold customer_risk: {spark.table('gold_customer_risk').count()} rows")

spark.sql("""
          SELECT
          customer_id,customer_name,city,total_transactions,total_spend,
          avg_transaction,max_transaction,fraud_count,avg_risk_score,first_transaction_date,
          active_days,customer_risk_label,spend_rank_in_city,gold_processed_ts
          FROM gold_customer_risk
          ORDER BY fraud_count DESC, avg_risk_score DESC
          LIMIT 10
""").show()

#%%
# ------------- GOLD TABLE 3: Merchant Fraud Analysis ----------
spark.sql("""
          CREATE OR REPLACE TEMP VIEW gold_merchant_fraud_view AS
          SELECT
          merchant,
          COUNT(*)AS total_transactions,
          COUNT(DISTINCT customer_id) AS unique_customers,
          ROUND(SUM(amount), 2)AS total_volume,
          ROUND(AVG(amount), 2) AS avg_transaction,
          SUM(is_fraud) AS fraud_transactions,
          ROUND(SUM(is_fraud)*100.0/count(*),2) AS fraud_rate_pct,
          ROUND(SUM(
            CASE WHEN is_fraud = 1 THEN amount ELSE 0 END), 2)  AS fraud_volume,
          RANK() OVER (ORDER BY SUM(is_fraud) DESC) AS fraud_rank,
          current_timestamp() AS gold_processed_ts
    FROM silver_transactions
    GROUP BY merchant
""")

spark.table("gold_merchant_fraud_view").write.format("delta").mode("overwrite").saveAsTable("gold_merchant_fraud")

print(f"Gold merchant_fraud: {spark.table('gold_merchant_fraud').count()} rows")

spark.sql("""
    SELECT
        merchant, total_transactions,
        fraud_transactions, fraud_rate_pct,
        fraud_volume, fraud_rank
    FROM gold_merchant_fraud
    ORDER BY fraud_rank
""").show()
#%%
#------------ Gold Layer Summary ----------------
spark.sql("""
    SELECT 'gold_daily_summary'  AS table_name, COUNT(*) AS row_count
    FROM gold_daily_summary
    UNION ALL
    SELECT 'gold_customer_risk', COUNT(*)
    FROM gold_customer_risk
    UNION ALL
    SELECT 'gold_merchant_fraud', COUNT(*)
    FROM gold_merchant_fraud
""").show()
