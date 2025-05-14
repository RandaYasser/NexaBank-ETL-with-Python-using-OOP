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
-- Previous configuration settings remain the same...

CREATE DATABASE IF NOT EXISTS nexabank
LOCATION '/user/hive/warehouse/nexabank.db';

USE nexabank;

-- Step 1: Create temporary staging table
CREATE EXTERNAL TABLE tmp_credit_cards_billing (
    bill_id STRING,
    customer_id STRING,
    month STRING,
    amount_due DOUBLE,
    amount_paid DOUBLE,
    payment_date STRING,
    due_date STRING,
    late_days INT,
    fully_paid BOOLEAN,
    debt DOUBLE,
    fine DOUBLE,
    total_amount DOUBLE,
    processing_time STRING,
    partition_date STRING,
    partition_hour STRING
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/nexabank.db/credit_cards_billing';

-- Step 2: Create final table
CREATE EXTERNAL TABLE credit_cards_billing (
    bill_id STRING,
    customer_id STRING,
    month TIMESTAMP,
    amount_due DOUBLE,
    amount_paid DOUBLE,
    payment_date TIMESTAMP,
    due_date TIMESTAMP,
    late_days INT,
    fully_paid BOOLEAN,
    debt DOUBLE,
    fine DOUBLE,
    total_amount DOUBLE,
    processing_time TIMESTAMP,
    partition_date DATE,
    partition_hour INT
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/nexabank.db/credit_cards_billing_final'
TBLPROPERTIES (
  'parquet.timestamp.int96.enabled'='false',
  'parquet.timestamp.legacy.conversion.enabled'='false'
);

-- Step 3: Insert with timestamp conversion
INSERT INTO credit_cards_billing
SELECT 
    bill_id,
    customer_id,
    CAST(month AS TIMESTAMP) AS month,
    amount_due,
    amount_paid,
    CAST(payment_date AS TIMESTAMP) AS payment_date,
    CAST(due_date AS TIMESTAMP) AS due_date,
    late_days,
    fully_paid,
    debt,
    fine,
    total_amount,
    CAST(processing_time AS TIMESTAMP) AS processing_time,
    CAST(partition_date AS DATE) AS partition_date,
    CAST(partition_hour AS INT) AS partition_hour
FROM tmp_credit_cards_billing;

-- Step 4: Verify
SELECT * FROM credit_cards_billing LIMIT 5;