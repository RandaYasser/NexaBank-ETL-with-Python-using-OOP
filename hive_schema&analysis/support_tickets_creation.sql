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
CREATE EXTERNAL TABLE tmp_support_tickets (
    ticket_id STRING,
    customer_id STRING,
    complaint_category STRING,
    complaint_date STRING,
    severity INT,
    age INT,
    processing_time STRING,
    partition_date STRING,
    partition_hour STRING
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/nexabank.db/support_tickets';

-- Step 2: Create final table
CREATE EXTERNAL TABLE support_tickets (
    ticket_id STRING,
    customer_id STRING,
    complaint_category STRING,
    complaint_date TIMESTAMP,
    severity INT,
    age INT,
    processing_time TIMESTAMP,
    partition_date DATE,
    partition_hour INT
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/nexabank.db/support_tickets_final'
TBLPROPERTIES (
  'parquet.timestamp.int96.enabled'='false',
  'parquet.timestamp.legacy.conversion.enabled'='false'
);

-- Step 3: Insert with timestamp conversion
INSERT INTO support_tickets
SELECT 
    ticket_id,
    customer_id,
    complaint_category,
    CAST(complaint_date AS TIMESTAMP) AS complaint_date,
    severity,
    age,
    CAST(processing_time AS TIMESTAMP) AS processing_time,
    CAST(partition_date AS DATE) AS partition_date,
    CAST(partition_hour AS INT) AS partition_hour
FROM tmp_support_tickets;

-- Step 4: Verify
SELECT * FROM support_tickets LIMIT 5;