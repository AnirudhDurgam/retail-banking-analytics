#%% md
# # 02 - Silver Transformation
# ### Spark SQL for all transformations
# 
#%%
# ------------- STEP 1: Remove nulls ------------------

spark.sql("""
          CREATE OR REPLACE TEMP VIEW remove_nulls AS
          SELECT * 
          FROM bronze_transactions
          WHERE amount IS NOT NULL 
          AND Customer_id IS NOT NULL 
          AND transaction_id IS NOT NULL
""")
spark.sql("""
          SELECT count(*) AS Non_Null 
          FROM remove_nulls
""").show()


#%%
#------------ STEP 2 Remove Invalid Negatives (keep refund)-----------------

spark.sql("""
          CREATE OR REPLACE TEMP VIEW remove_negatives AS
          SELECT * 
          FROM remove_nulls
          WHERE amount >= 0
          OR transaction_type ="Refund"
""")
spark.sql("""
          SELECT COUNT(*)
          FROM remove_negatives
""").show()

#%%

#------------ STEP 3: De-Duplication using ROW_NUM ------------------

spark.sql("""
          CREATE OR REPLACE TEMP VIEW de_duplicated AS
          SELECT * EXCEPT (row_number)
          FROM(
            SELECT *,
            row_number() OVER(
              PARTITION BY transaction_id
              order by transaction_timestamp 
            ) AS row_number
            FROM remove_negatives
          )
          WHERE row_number=1          
""")

spark.sql("""
          SELECT COUNT(*) as de_deuplicated_count 
          FROM de_duplicated
""").show()
#%%
#------------- STEP 4 : Type Casting and Derived Colums ------------------

spark.sql("""
          CREATE OR REPLACE TEMP VIEW enriched_data AS
          SELECT
            transaction_id,
            customer_id,
            ROUND(CAST(amount AS DOUBLE),2) AS amount,
            currency,
            merchant,
            transaction_type,
            city,
            channel,
            CAST(is_fraud AS INT) AS is_fraud,
            to_timestamp(transaction_timestamp,'yyyy-MM-dd HH:mm:ss') AS transaction_timestamp,
            to_date(transaction_timestamp,'yyyy-MM-dd HH:mm:ss') AS transaction_date,
            hour(to_timestamp(transaction_timestamp)) AS transaction_hour,
            ROUND(CAST(fraud_score AS DOUBLE),2) AS risk_score,
            CASE 
                WHEN amount < 500 THEN 'micro'
                WHEN amount < 5000 THEN 'small'
                WHEN amount < 25000 THEN 'medium'
                WHEN amount < 100000 THEN 'large'
                ELSE 'high_value'
            END AS amount_bucket,
            CASE
                WHEN dayofweek(to_date(transaction_timestamp, 'yyyy-MM-dd HH:mm:ss'))
                IN (1,7) THEN 1 
                ELSE 0 
            END AS is_weekend,
            CASE 
                WHEN fraud_score >= 75 THEN 'high_risk'
                WHEN fraud_score >= 40 THEN 'medium_risk'
                ELSE 'low_risk'
            END AS risk_category,
            current_timestamp() AS silver_processed_TS
            FROM de_duplicated
          """)
spark.sql("""
          SELECT count(*) as FINAL_Silver_Count FROM enriched_data
          """).show()
#%%
#------------STEP 5 :  Preview Enriched Data -----------------
spark.sql("""
          SELECT 
          transaction_id, transaction_type,amount,amount_bucket, risk_category, is_fraud,transaction_date, transaction_hour
          FROM enriched_data LIMIT 10 
          """).show()
#%%
# ---- STEP 6: Data quality assertion ----------------
quality_check = spark.sql("""
    SELECT
        SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS null_txn_ids,
        SUM(CASE WHEN customer_id IS NULL    THEN 1 ELSE 0 END) AS null_cust_ids,
        SUM(CASE WHEN amount < 0 AND transaction_type != 'refund'
                 THEN 1 ELSE 0 END)                             AS invalid_negatives
    FROM enriched_data
""")
quality_check.show()
#%%
#-------- STEP 7 : Writing/Cteating Silver Table -----------------
spark.table("enriched_data").write.format("delta").mode("overwrite").partitionBy("transaction_date").saveAsTable("silver_transactions")
print(f"Silver Transaction written: {spark.table('silver_transactions').count()} rows")
#%%
#------------ STEP 8 : Clean Silver Customer Table ----------------

spark.sql("""
           CREATE OR REPLACE TEMP VIEW customer_silver_clean AS 
           SELECT 
           customer_id,
           customer_name,
           city,
           account_type,
           round(cast(balance as double),2) AS account_balance,
           status,
           TO_DATE(creation_date,'yyyy-MM-dd') AS account_opened_date,
           current_timestamp() AS silver_processed_ts
           FROM bronze_customers
           WHERE customer_id is NOT NULL
           """)

spark.table("customer_silver_clean").write.format("delta").mode("overwrite").saveAsTable("silver_customers")

print(f"Silver Customers written: {spark.table('silver_customers').count()} rows")
#%%
#------------------- Bronze Vs Silver Transactions Comparision ---------------------------

spark.sql("""
          SELECT 'bronze_transactions' AS layer , count (*) AS row_count
          FROM bronze_transactions
          UNION ALL
          SELECT 'silver_transactions' , count(*)
          FROM silver_transactions
          """).show()