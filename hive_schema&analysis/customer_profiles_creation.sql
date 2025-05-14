DROP TABLE IF EXISTS nexabank.transactions
DROP TABLE IF EXISTS nexabank.loans
DROP TABLE IF EXISTS nexabank.customer_profiles 
DROP TABLE IF EXISTS nexabank.support_tickets
DROP TABLE IF EXISTS nexabank.credit_cards_billing;



SELECT * FROM nexabank.transactions LIMIT 100;
SELECT * FROM nexabank.loans LIMIT 100;
SELECT * FROM nexabank.customer_profiles LIMIT 100;
SELECT * FROM nexabank.support_tickets LIMIT 100;
SELECT * FROM nexabank.credit_cards_billing LIMIT 100;




SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.support.concurrency=true;
SET hive.enforce.bucketing=true;
SET hive.compactor.initiator.on=true;
SET hive.compactor.worker.threads=1;

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;


-- Set these before your INSERT statement
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=5000;  -- Global maximum
SET hive.exec.max.dynamic.partitions.pernode=2000;  -- Per node maximum
SET hive.vectorized.execution.enabled=false;

SET hive.exec.max.created.files=100000; -- Prevent OOM during large loads


SET hive.exec.orc.default.stripe.size=268435456; -- 256MB optimal stripe size
SET hive.exec.orc.default.compress=ZSTD; -- Best compression ratio


----------------------------------------------------------------------
-- Previous configuration and DROP statements remain the same...

CREATE DATABASE IF NOT EXISTS nexabank
LOCATION '/user/hive/warehouse/nexabank.db';

USE nexabank;

-- Step 1: Create temporary staging table
CREATE EXTERNAL TABLE tmp_customer_profiles (
    customer_id STRING,
    name STRING,
    gender STRING,
    age INT,
    city STRING,
    account_open_date STRING,
    product_type STRING,
    customer_tier STRING,
    tenure INT,
    customer_segment STRING,
    processing_time STRING,
    partition_date STRING,
    partition_hour STRING
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/nexabank.db/customer_profiles';

-- Step 2: Create final table with proper timestamp
CREATE EXTERNAL TABLE customer_profiles (
    customer_id STRING,
    name STRING,
    gender STRING,
    age INT,
    city STRING,
    account_open_date TIMESTAMP,
    product_type STRING,
    customer_tier STRING,
    tenure INT,
    customer_segment STRING,
    processing_time TIMESTAMP,
    partition_date DATE,
    partition_hour INT
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/nexabank.db/customer_profiles_final'
TBLPROPERTIES (
  'parquet.timestamp.int96.enabled'='false',
  'parquet.timestamp.legacy.conversion.enabled'='false'
);

-- Step 3: Insert with timestamp conversion
INSERT INTO customer_profiles
SELECT 
    customer_id,
    name,
    gender,
    age,
    city,
    CAST(account_open_date AS TIMESTAMP) AS account_open_date,
    product_type,
    customer_tier,
    tenure,
    customer_segment,
    CAST(processing_time AS TIMESTAMP) AS processing_time,
    CAST(partition_date AS DATE) AS partition_date,
    CAST(partition_hour AS INT) AS partition_hour
FROM tmp_customer_profiles;

-- Step 4: Verify
SELECT * FROM customer_profiles LIMIT 5;