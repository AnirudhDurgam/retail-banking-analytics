#%% md
# # **Retail Banking Analytics**
# ### Setup File , Data generation and ingestion (dirty data) for the demo, Bronze layer 
# 
#%%

#import
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp,current_date, lit
import random 
from datetime import datetime,timedelta

spark=SparkSession.builder.appName("RetailBankingAnalytics").getOrCreate()

#%%
# ------------------------------   Configuration   -----------------------------------

Num_Customers    = 500
Num_transactions = 50000
Start_Date       = datetime(2025,1,1)
Merchants        = ["Amazon","Swiggy","Zomato","Flipkart","BigBazaar","Reliance","DMart","Myntra","BookMyShow","Uber","Ola","Netflix","Hotstar","PhonePe","Paytm","InstaMart","Blinkit"]
CITIES           = ["Mumbai","Delhi","Bangalore","Hyderabad","Chennai","Pune","Kolkata","Ahmedabad","Jaipur","Surat"]
TXN_Types        = ["Purchase","Withdraw","Refund","Transfer","Deposit"]
CHANNELS         = ["mobile_app","web","ATM","branch","UPI"]
ACCOUNT_TYPES    = ["savings","current","salary"]
#%%
#------------------------------  Generate Customers  -----------------------------
customers =[]
for i in range (1,Num_Customers+1):
    customers.append((
        f"CUST{i:05d}",
        f"CUSTOMER_{i}",
        random.choice(CITIES),
        random.choice(ACCOUNT_TYPES),
        round(random.uniform(1000,500000),2),
        random.choice(["active","active","active","inactive"]),
        (Start_Date + timedelta(days=random.randint(0,1000))).strftime("%Y-%m-%d")
    ))

cust_schema = StructType([
    StructField("customer_id",StringType(),False),
    StructField("customer_name",StringType(),True),
    StructField("city",StringType(),True),
    StructField("account_type",StringType(),True),
    StructField("balance",FloatType(),True),
    StructField("status",StringType(),True),
    StructField("creation_date",StringType(),True)
])

customers_df = spark.createDataFrame(customers,schema=cust_schema)
print(f"Customers Generated : {customers_df.count()}")

#%%

#%%
#--------------------------------  Generate Transaction data + Dirty Data ingestion ----------------------------------
transactions =[]
for i in range(1,Num_transactions):
  txn_date = Start_Date + timedelta(
    days=random.randint(0,364),
    hours=random.randint(0,23),
    minutes= random.randint(0,59)
  )
  is_dirty=random.random() < 0.08

  row =(
    f"TXN{i:08d}",
    f"CUST{random.randint(1,Num_Customers):05d}",
    random.choice(TXN_Types),
    None if (is_dirty and random.random() < 0.3 )
    else round(random.uniform(-500,50000),2),
    "INR",
    random.choice(Merchants),
    random.choice(CITIES),
    random.choice(CHANNELS),
    1 if random.random() < 0.02 else 0,
    txn_date.strftime("%Y-%m-%d %H:%M:%S"),
    "Duplicate" if (is_dirty and random.random() <0.4) else "Original",
    round(random.uniform(0,100),2)
  )
  transactions.append(row)

  # ------- Dirty Data Ingestion : Duplicate rows ----
  if random.random() < 0.03 :
    dup =list(row)
    dup[9] = (txn_date + timedelta(seconds=random.randint(1,60))).strftime("%Y-%m-%d %H:%M:%S")
    dup[10]="Duplicate"
    transactions.append(tuple(dup))

txn_schema=StructType([
  StructField("transaction_id",StringType(),False),
  StructField("Customer_id",StringType(),True),
  StructField("transaction_type",StringType(),True),
  StructField("amount",DoubleType(),True),
  StructField("currency",StringType(),True),
  StructField("merchant",StringType(),True),
  StructField("city",StringType(),True),
  StructField("channel",StringType(),True),
  StructField("is_fraud",IntegerType(),True),
  StructField("transaction_timestamp",StringType(),True),
  StructField("record_type",StringType(),True),
  StructField("fraud_score",DoubleType(),True)
])

transactions_df = spark.createDataFrame(transactions,schema=txn_schema)

print(f"Transactions Generated : {transactions_df.count()}")
print(f"Null Amounts           : {transactions_df.filter('amount IS NULL').count()}")
print(f"Fraud Transactions     : {transactions_df.filter('is_fraud=1').count()}")
print(f"Duplicate Rows         : {transactions_df.filter('''record_type = 'Duplicate' ''').count()}")
#%%
# ----------------- Save Data frames as Managed DELTA TABLES --------------------

transactions_df.write.format("delta").mode("overwrite").saveAsTable("raw_transactions")
customers_df.write.format("delta").mode("overwrite").saveAsTable("raw_customers")
#%%
# ------------------- Create BRONZE table and add METADATA USING SQL ----------------------------------
spark.sql("""
          CREATE OR REPLACE TABLE bronze_transactions AS 
          SELECT 
            *,
            current_timestamp() AS ingestion_date,
            current_date() AS load_date,
            'retail_banking_v1'  AS source_system
          FROM raw_transactions
""")

spark.sql("""
          CREATE OR REPLACE TABLE bronze_customers AS
          SELECT 
            *,
            current_timestamp() AS ingestion_date,
            current_date() AS load_date,
            'retail_banking_v1' AS source_system
            FROM raw_customers

          """)


#%%
#-----------------   Brone Table Quality Check ------------------

spark.sql("""
          SELECT
            Count(*) AS Total_records,
            SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amounts,
            SUM(CASE WHEN Customer_id IS NULL THEN 1 ELSE 0 END) As null_customers,
            SUM(CASE WHEN record_type='Duplicate' THEN 1 ELSE 0 END) AS flagged_duplicates,
            SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) AS negative_amount,
            SUM(CASE WHEN is_fraud=1 THEN 1 ELSE 0 END) AS fraud_flagged
          FROM bronze_transactions
          """).show()

#------------- Verify Table Exists --------------------
spark.sql("SHOW TABLES").show()
